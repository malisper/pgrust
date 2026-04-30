use super::super::*;
use super::alter_table_work_queue::{build_alter_table_work_queue, has_inheritance_children};
use crate::backend::commands::tablecmds::collect_matching_rows_heap;
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
    ensure_relation_owner, is_system_column_name, lookup_table_or_partitioned_table_for_alter_table,
};

fn relation_basename(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

fn relation_by_oid_for_constraint_command(
    db: &Database,
    client_id: ClientId,
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Option<crate::backend::parser::BoundRelation> {
    catalog.relation_by_oid(relation_oid).or_else(|| {
        db.lazy_catalog_lookup(client_id, None, None)
            .relation_by_oid(relation_oid)
    })
}

fn inherited_foreign_key_alter_error(
    row: &PgConstraintRow,
    relation_name: &str,
    catalog: &dyn CatalogLookup,
) -> ExecError {
    let parent = (row.conparentid != 0)
        .then(|| catalog.constraint_row_by_oid(row.conparentid))
        .flatten();
    let parent_relation_name = parent
        .as_ref()
        .and_then(|parent| catalog.class_row_by_oid(parent.conrelid))
        .map(|class| class.relname);

    ExecError::DetailedError {
        message: format!(
            "cannot alter constraint \"{}\" on relation \"{}\"",
            row.conname, relation_name
        ),
        detail: parent.as_ref().and_then(|parent| {
            parent_relation_name.as_ref().map(|relname| {
                format!(
                    "Constraint \"{}\" is derived from constraint \"{}\" of relation \"{}\".",
                    row.conname, parent.conname, relname
                )
            })
        }),
        hint: parent.map(|_| "You may alter the constraint it derives from instead.".into()),
        sqlstate: "55000",
    }
}

fn lookup_table_partitioned_or_view_for_alter_table(
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
            push_notice(format!(r#"relation "{name}" does not exist, skipping"#));
            Ok(None)
        }
        None => Err(ExecError::Parse(ParseError::UnknownTable(name.to_string()))),
    }
}

fn unsupported_view_alter_action(action: &str, relation_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "ALTER action {action} cannot be performed on relation \"{}\"",
            relation_basename(relation_name)
        ),
        detail: Some("This operation is not supported for views.".into()),
        hint: None,
        sqlstate: "42809",
    }
}

fn reject_constraint_with_dependent_rule(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    constraint_oid: u32,
    constraint_name: &str,
    table_name: &str,
) -> Result<(), ExecError> {
    let depends =
        crate::backend::utils::cache::syscache::ensure_depend_rows(db, client_id, txn_ctx);
    let Some(depend) = depends.into_iter().find(|depend| {
        depend.classid == crate::include::catalog::PG_REWRITE_RELATION_OID
            && depend.refclassid == crate::include::catalog::PG_CONSTRAINT_RELATION_OID
            && depend.refobjid == constraint_oid
            && depend.deptype == crate::include::catalog::DEPENDENCY_NORMAL
    }) else {
        return Ok(());
    };
    let rewrites =
        crate::backend::utils::cache::syscache::ensure_rewrite_rows(db, client_id, txn_ctx);
    let classes = crate::backend::utils::cache::syscache::ensure_class_rows(db, client_id, txn_ctx);
    let view_name = rewrites
        .iter()
        .find(|rewrite| rewrite.oid == depend.objid)
        .and_then(|rewrite| {
            classes
                .iter()
                .find(|class| class.oid == rewrite.ev_class)
                .map(|class| class.relname.clone())
        })
        .unwrap_or_else(|| "unknown".into());
    Err(ExecError::DetailedError {
        message: format!(
            "cannot drop constraint {constraint_name} on table {table_name} because other objects depend on it"
        ),
        detail: Some(format!(
            "view {view_name} depends on constraint {constraint_name} on table {table_name}"
        )),
        hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
        sqlstate: "2BP01",
    })
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

fn choose_partition_clone_constraint_name(
    base: &str,
    used_names: &mut std::collections::BTreeSet<String>,
) -> String {
    for suffix in 1.. {
        let candidate = format!("{base}_{suffix}");
        if used_names.insert(candidate.to_ascii_lowercase()) {
            return candidate;
        }
    }
    unreachable!("constraint name suffix space exhausted")
}

fn ddl_not_null_contains_null_error(relation_name: &str, column_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "column \"{column_name}\" of relation \"{relation_name}\" contains null values"
        ),
        detail: None,
        hint: None,
        sqlstate: "23502",
    }
}

fn incompatible_not_valid_not_null_error(constraint_name: &str, relation_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "incompatible NOT VALID constraint \"{constraint_name}\" on relation \"{relation_name}\""
        ),
        detail: None,
        hint: Some(
            "You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT.".into(),
        ),
        sqlstate: "55000",
    }
}

fn cannot_change_not_null_no_inherit_error(
    constraint_name: &str,
    relation_name: &str,
    include_hint: bool,
) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "cannot change NO INHERIT status of NOT NULL constraint \"{constraint_name}\" on relation \"{relation_name}\""
        ),
        detail: None,
        hint: include_hint.then(|| {
            "You might need to make the existing constraint inheritable using ALTER TABLE ... ALTER CONSTRAINT ... INHERIT.".into()
        }),
        sqlstate: "0A000",
    }
}

fn not_null_pk_incompatible_error(
    row: &PgConstraintRow,
    column_name: &str,
    relation_name: &str,
    marker: &str,
) -> ExecError {
    let hint = if marker == "NO INHERIT" {
        "You might need to make the existing constraint inheritable using ALTER TABLE ... ALTER CONSTRAINT ... INHERIT."
    } else {
        "You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT."
    };
    ExecError::DetailedError {
        message: format!("cannot create primary key on column \"{column_name}\""),
        detail: Some(format!(
            "The constraint \"{}\" on column \"{}\" of table \"{}\", marked {}, is incompatible with a primary key.",
            row.conname, column_name, relation_name, marker,
        )),
        hint: Some(hint.into()),
        sqlstate: "55000",
    }
}

pub(super) fn verify_not_null_pk_compatible(
    row: &PgConstraintRow,
    column_name: &str,
    relation_name: &str,
) -> Result<(), ExecError> {
    if row.connoinherit {
        return Err(not_null_pk_incompatible_error(
            row,
            column_name,
            relation_name,
            "NO INHERIT",
        ));
    }
    if !row.convalidated {
        return Err(not_null_pk_incompatible_error(
            row,
            column_name,
            relation_name,
            "NOT VALID",
        ));
    }
    Ok(())
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
    _catalog: &dyn CatalogLookup,
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
        active_grouping_refs: Vec::new(),
        subplans: Vec::new(),
        timed: false,
        allow_side_effects: false,
        pending_async_notifications: Vec::new(),
        catalog_effects: Vec::new(),
        temp_effects: Vec::new(),
        database: Some(db.clone()),
        pending_catalog_effects: Vec::new(),
        pending_table_locks: Vec::new(),
        pending_portals: Vec::new(),
        catalog: None,
        scalar_function_cache: std::collections::HashMap::new(),
        srf_rows_cache: std::collections::HashMap::new(),
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
    _constraint_name: &str,
    catalog: &dyn CatalogLookup,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    interrupts: std::sync::Arc<crate::backend::utils::misc::interrupts::InterruptState>,
) -> Result<(), ExecError> {
    if matches!(relation.relkind, 'f' | 'p') {
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
            return Err(ddl_not_null_contains_null_error(
                relation_name,
                &column_name,
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_check_rows<C: CatalogLookup + Clone + 'static>(
    db: &Database,
    relation: &crate::backend::parser::BoundRelation,
    relation_name: &str,
    constraint_name: &str,
    expr_sql: &str,
    catalog: &C,
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
        validated: true,
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
    ctx.catalog = Some(crate::backend::executor::executor_catalog(catalog.clone()));
    let rows =
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, &mut ctx)?;
    for (_, values) in rows {
        let mut slot =
            TupleSlot::virtual_row_with_metadata(values, None, Some(relation.relation_oid));
        match eval_expr(&check.expr, &mut slot, &mut ctx)? {
            Value::Null | Value::Bool(true) => {}
            Value::Bool(false) => {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "check constraint \"{}\" of relation \"{}\" is violated by some row",
                        check.constraint_name, relation_name
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "23514",
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

fn constraint_index_columns_with_expr_types(
    action: &crate::backend::parser::IndexBackedConstraintAction,
    relation: &crate::backend::parser::BoundRelation,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<crate::backend::parser::IndexColumnDef>, ExecError> {
    let mut columns = if action.index_columns.is_empty() {
        action
            .columns
            .iter()
            .cloned()
            .map(crate::backend::parser::IndexColumnDef::from)
            .collect::<Vec<_>>()
    } else {
        action.index_columns.clone()
    };
    for column in &mut columns {
        if let Some(expr_sql) = column.expr_sql.as_deref()
            && column.expr_type.is_none()
        {
            column.expr_type = Some(
                crate::backend::parser::infer_relation_expr_sql_type(
                    expr_sql,
                    None,
                    &relation.desc,
                    catalog,
                )
                .map_err(ExecError::Parse)?,
            );
        }
    }
    Ok(columns)
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

fn attnums_by_parent_column_names(
    parent_desc: &crate::backend::executor::RelationDesc,
    child_desc: &crate::backend::executor::RelationDesc,
    parent_attnums: &[i16],
) -> Result<Vec<i16>, ExecError> {
    parent_attnums
        .iter()
        .map(|attnum| {
            let index = usize::try_from(attnum.saturating_sub(1)).map_err(|_| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "user column attnum",
                    actual: attnum.to_string(),
                })
            })?;
            let parent_column = parent_desc.columns.get(index).ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "user column attnum",
                    actual: attnum.to_string(),
                })
            })?;
            child_desc
                .columns
                .iter()
                .enumerate()
                .find_map(|(child_index, child_column)| {
                    (!child_column.dropped
                        && child_column.name.eq_ignore_ascii_case(&parent_column.name))
                    .then_some(child_index as i16 + 1)
                })
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnknownColumn(parent_column.name.clone()))
                })
        })
        .collect()
}

fn index_key_attnums(index: &crate::backend::parser::BoundIndexRelation) -> Option<Vec<i16>> {
    let key_count = usize::try_from(index.index_meta.indnkeyatts.max(0)).ok()?;
    if key_count > index.index_meta.indkey.len() {
        return None;
    }
    Some(
        index
            .index_meta
            .indkey
            .iter()
            .take(key_count)
            .copied()
            .collect(),
    )
}

fn find_referenced_foreign_key_index(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    attnums: &[i16],
    conperiod: bool,
) -> Option<crate::backend::parser::BoundIndexRelation> {
    if conperiod {
        let constraint = catalog
            .constraint_rows_for_relation(relation_oid)
            .into_iter()
            .find(|row| {
                matches!(row.contype, CONSTRAINT_PRIMARY | CONSTRAINT_UNIQUE)
                    && row.conperiod
                    && row.conindid != 0
                    && row.conkey.as_deref() == Some(attnums)
            })?;
        return catalog
            .index_relations_for_heap(relation_oid)
            .into_iter()
            .find(|index| {
                index.relation_oid == constraint.conindid
                    && index.index_meta.indisvalid
                    && index.index_meta.indisready
                    && index.index_meta.indisexclusion
            });
    }

    catalog
        .index_relations_for_heap(relation_oid)
        .into_iter()
        .find(|index| {
            index.index_meta.indisunique
                && index.index_meta.indisvalid
                && index.index_meta.indisready
                && index.index_meta.am_oid == crate::include::catalog::BTREE_AM_OID
                && index_key_attnums(index).is_some_and(|key_attnums| key_attnums == attnums)
                && !index
                    .index_meta
                    .indpred
                    .as_deref()
                    .is_some_and(|pred| !pred.is_empty())
                && !index
                    .index_meta
                    .indexprs
                    .as_deref()
                    .is_some_and(|exprs| !exprs.is_empty())
        })
}

fn is_referenced_side_foreign_key_clone(
    row: &PgConstraintRow,
    catalog: &dyn CatalogLookup,
) -> bool {
    if row.contype != CONSTRAINT_FOREIGN || row.conparentid == 0 {
        return false;
    }
    catalog
        .constraint_row_by_oid(row.conparentid)
        .is_some_and(|parent| parent.conrelid == row.conrelid)
}

fn can_spawn_referenced_partition_foreign_key_clone(
    row: &PgConstraintRow,
    catalog: &dyn CatalogLookup,
) -> bool {
    row.contype == CONSTRAINT_FOREIGN
        && (row.conparentid == 0 || is_referenced_side_foreign_key_clone(row, catalog))
}

fn partition_descendants(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Vec<crate::backend::parser::BoundRelation>, ExecError> {
    let mut descendants = Vec::new();
    let mut queue = std::collections::VecDeque::from([relation_oid]);
    while let Some(parent_oid) = queue.pop_front() {
        for relation in sorted_partition_child_relations(catalog, parent_oid)? {
            queue.push_back(relation.relation_oid);
            descendants.push(relation);
        }
    }
    Ok(descendants)
}

fn sorted_partition_child_relations(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Vec<crate::backend::parser::BoundRelation>, ExecError> {
    let mut children = catalog
        .inheritance_children(relation_oid)
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .map(|row| {
            catalog
                .relation_by_oid(row.inhrelid)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("missing partition relation {}", row.inhrelid),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    children.sort_by(|left, right| {
        let left_name = catalog
            .class_row_by_oid(left.relation_oid)
            .map(|row| row.relname)
            .unwrap_or_default();
        let right_name = catalog
            .class_row_by_oid(right.relation_oid)
            .map(|row| row.relname)
            .unwrap_or_default();
        partition_relation_sort_key(left, &left_name)
            .cmp(&partition_relation_sort_key(right, &right_name))
    });
    Ok(children)
}

fn partition_relation_sort_key(
    relation: &crate::backend::parser::BoundRelation,
    relation_name: &str,
) -> (bool, String, u32) {
    let bound = relation.relpartbound.clone().unwrap_or_default();
    let is_default = relation_name.to_ascii_lowercase().contains("default")
        || crate::backend::commands::partition::partition_relation_is_default(relation)
            .unwrap_or_else(|_| partition_bound_text_is_default(&bound));
    (is_default, bound, relation.relation_oid)
}

fn partition_bound_text_is_default(bound: &str) -> bool {
    let lower_bound = bound.to_ascii_lowercase();
    if lower_bound.contains("\"is_default\":true")
        || lower_bound.contains("\"is_default\": true")
        || lower_bound.contains("default")
    {
        return true;
    }
    serde_json::from_str::<serde_json::Value>(bound)
        .ok()
        .is_some_and(|value| json_value_has_true_is_default(&value))
}

fn json_value_has_true_is_default(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => {
            map.get("is_default")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
                || map.values().any(json_value_has_true_is_default)
        }
        serde_json::Value::Array(values) => values.iter().any(json_value_has_true_is_default),
        _ => false,
    }
}

fn partition_leaf_descendants(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Vec<crate::backend::parser::BoundRelation>, ExecError> {
    Ok(partition_descendants(catalog, relation_oid)?
        .into_iter()
        .filter(|relation| relation.relkind != 'p')
        .collect())
}

fn foreign_key_validation_targets(
    catalog: &dyn CatalogLookup,
    relation: &crate::backend::parser::BoundRelation,
) -> Result<Vec<crate::backend::parser::BoundRelation>, ExecError> {
    if relation.relkind == 'p' {
        return partition_leaf_descendants(catalog, relation.relation_oid);
    }
    Ok(vec![relation.clone()])
}

fn column_indexes_for_names(
    desc: &crate::backend::executor::RelationDesc,
    names: &[String],
) -> Result<Vec<usize>, ExecError> {
    names
        .iter()
        .map(|column_name| {
            desc.columns
                .iter()
                .enumerate()
                .find_map(|(index, column)| {
                    (!column.dropped && column.name.eq_ignore_ascii_case(column_name))
                        .then_some(index)
                })
                .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.clone())))
        })
        .collect()
}

fn optional_column_index_for_name(
    desc: &crate::backend::executor::RelationDesc,
    name: &Option<String>,
) -> Result<Option<usize>, ExecError> {
    name.as_ref()
        .map(|column_name| {
            desc.columns
                .iter()
                .enumerate()
                .find_map(|(index, column)| {
                    (!column.dropped && column.name.eq_ignore_ascii_case(column_name))
                        .then_some(index)
                })
                .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.clone())))
        })
        .transpose()
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
    datetime_config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
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
    let mut ctx = ddl_executor_context(
        db,
        catalog,
        client_id,
        xid,
        cid,
        datetime_config,
        interrupts,
    )?;
    if referenced_relation.relkind == 'p' {
        ctx.catalog = Some(crate::backend::executor::executor_catalog(
            db.lazy_catalog_lookup(client_id, None, None),
        ));
    }
    let referenced_column_indexes =
        column_indexes_for_names(&referenced_relation.desc, &action.referenced_columns)?;
    let referenced_period_column_index =
        optional_column_index_for_name(&referenced_relation.desc, &action.referenced_period)?;
    let validation_targets = foreign_key_validation_targets(catalog, relation)?;
    for target in validation_targets {
        let target_name = catalog
            .class_row_by_oid(target.relation_oid)
            .map(|class| class.relname)
            .unwrap_or_else(|| relation_name.to_string());
        let constraint = BoundForeignKeyConstraint {
            constraint_oid: 0,
            constraint_name: action.constraint_name.clone(),
            relation_name: target_name.clone(),
            column_names: action.columns.clone(),
            column_indexes: column_indexes_for_names(&target.desc, &action.columns)?,
            period_column_index: optional_column_index_for_name(&target.desc, &action.period)?,
            match_type: action.match_type,
            referenced_relation_name: action.referenced_table.clone(),
            referenced_relation_oid: referenced_relation.relation_oid,
            referenced_rel: referenced_relation.rel,
            referenced_toast: referenced_relation.toast,
            referenced_desc: referenced_relation.desc.clone(),
            referenced_column_indexes: referenced_column_indexes.clone(),
            referenced_period_column_index,
            referenced_index: referenced_index.clone(),
            deferrable: false,
            initially_deferred: false,
            enforced: true,
        };
        let rows =
            collect_matching_rows_heap(target.rel, &target.desc, target.toast, None, &mut ctx)?;
        for (_, values) in rows {
            crate::backend::executor::validate_outbound_foreign_key_for_ddl(
                &target_name,
                &constraint,
                &values,
                &mut ctx,
            )?;
        }
    }
    Ok(())
}

fn column_names_for_attnums(
    desc: &crate::backend::executor::RelationDesc,
    attnums: &[i16],
) -> Result<Vec<String>, ExecError> {
    attnums
        .iter()
        .map(|attnum| {
            let index = usize::try_from(attnum.saturating_sub(1)).map_err(|_| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "user column attnum",
                    actual: attnum.to_string(),
                })
            })?;
            desc.columns
                .get(index)
                .filter(|column| !column.dropped)
                .map(|column| column.name.clone())
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "user column attnum",
                        actual: attnum.to_string(),
                    })
                })
        })
        .collect()
}

fn foreign_key_action_from_catalog_code(code: char) -> Result<ForeignKeyAction, ExecError> {
    match code {
        'a' | ' ' => Ok(ForeignKeyAction::NoAction),
        'r' => Ok(ForeignKeyAction::Restrict),
        'c' => Ok(ForeignKeyAction::Cascade),
        'n' => Ok(ForeignKeyAction::SetNull),
        'd' => Ok(ForeignKeyAction::SetDefault),
        other => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "foreign-key action code",
            actual: other.to_string(),
        })),
    }
}

fn foreign_key_match_from_catalog_code(code: char) -> Result<ForeignKeyMatchType, ExecError> {
    match code {
        's' | ' ' => Ok(ForeignKeyMatchType::Simple),
        'f' => Ok(ForeignKeyMatchType::Full),
        'p' => Ok(ForeignKeyMatchType::Partial),
        other => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "foreign-key match code",
            actual: other.to_string(),
        })),
    }
}

fn foreign_key_validation_action_from_row(
    relation: &crate::backend::parser::BoundRelation,
    row: &PgConstraintRow,
    local_attnums: &[i16],
    referenced_attnums: &[i16],
    catalog: &dyn CatalogLookup,
) -> Result<ForeignKeyConstraintAction, ExecError> {
    let referenced_relation = catalog
        .relation_by_oid(row.confrelid)
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(row.confrelid.to_string())))?;
    let on_delete_set_columns = row
        .confdelsetcols
        .as_deref()
        .map(|attnums| column_names_for_attnums(&relation.desc, attnums))
        .transpose()?;
    let period = row
        .conperiod
        .then(|| {
            local_attnums
                .last()
                .copied()
                .map(|attnum| column_names_for_attnums(&relation.desc, &[attnum]))
                .transpose()
                .map(|names| names.and_then(|mut names| names.pop()))
        })
        .transpose()?
        .flatten();
    let referenced_period = row
        .conperiod
        .then(|| {
            referenced_attnums
                .last()
                .copied()
                .map(|attnum| column_names_for_attnums(&referenced_relation.desc, &[attnum]))
                .transpose()
                .map(|names| names.and_then(|mut names| names.pop()))
        })
        .transpose()?
        .flatten();
    Ok(ForeignKeyConstraintAction {
        constraint_name: row.conname.clone(),
        columns: column_names_for_attnums(&relation.desc, local_attnums)?,
        period,
        referenced_table: catalog
            .class_row_by_oid(referenced_relation.relation_oid)
            .map(|class| class.relname)
            .unwrap_or_else(|| row.confrelid.to_string()),
        referenced_relation_oid: referenced_relation.relation_oid,
        referenced_index_oid: row.conindid,
        self_referential: relation.relation_oid == referenced_relation.relation_oid,
        referenced_columns: column_names_for_attnums(
            &referenced_relation.desc,
            referenced_attnums,
        )?,
        referenced_period,
        match_type: foreign_key_match_from_catalog_code(row.confmatchtype)?,
        on_delete: foreign_key_action_from_catalog_code(row.confdeltype)?,
        on_delete_set_columns,
        on_update: foreign_key_action_from_catalog_code(row.confupdtype)?,
        deferrable: row.condeferrable,
        initially_deferred: row.condeferred,
        not_valid: !row.convalidated,
        enforced: row.conenforced,
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_attached_foreign_key_rows_if_needed(
    db: &Database,
    relation: &crate::backend::parser::BoundRelation,
    row: &PgConstraintRow,
    local_attnums: &[i16],
    referenced_attnums: &[i16],
    catalog: &dyn CatalogLookup,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    interrupts: std::sync::Arc<crate::backend::utils::misc::interrupts::InterruptState>,
) -> Result<(), ExecError> {
    if !row.conenforced {
        return Ok(());
    }
    let action = foreign_key_validation_action_from_row(
        relation,
        row,
        local_attnums,
        referenced_attnums,
        catalog,
    )?;
    let relation_name = catalog
        .class_row_by_oid(relation.relation_oid)
        .map(|class| class.relname)
        .unwrap_or_else(|| relation.relation_oid.to_string());
    validate_foreign_key_rows(
        db,
        relation,
        &relation_name,
        &action,
        catalog,
        client_id,
        xid,
        cid,
        &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        interrupts,
    )
}

fn optional_attnums_equal(left: Option<&[i16]>, right: Option<&[i16]>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        (None, None) => true,
        (Some(left), None) | (None, Some(left)) => left.is_empty(),
    }
}

fn foreign_key_attach_key_matches(
    child: &PgConstraintRow,
    parent: &PgConstraintRow,
    local_attnums: &[i16],
    referenced_attnums: &[i16],
    delete_set_attnums: Option<&[i16]>,
) -> bool {
    child.contype == CONSTRAINT_FOREIGN
        && child.confrelid == parent.confrelid
        && child.conkey.as_deref() == Some(local_attnums)
        && child.confkey.as_deref() == Some(referenced_attnums)
        && optional_attnums_equal(child.confdelsetcols.as_deref(), delete_set_attnums)
        && child.conperiod == parent.conperiod
}

fn foreign_key_attach_attributes_match(child: &PgConstraintRow, parent: &PgConstraintRow) -> bool {
    child.condeferrable == parent.condeferrable
        && child.condeferred == parent.condeferred
        && child.confupdtype == parent.confupdtype
        && child.confdeltype == parent.confdeltype
        && child.confmatchtype == parent.confmatchtype
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

fn not_null_constraint_for_attnum<'a>(
    rows: &'a [PgConstraintRow],
    attnum: i16,
) -> Option<&'a PgConstraintRow> {
    rows.iter().find(|row| {
        row.contype == CONSTRAINT_NOTNULL
            && row
                .conkey
                .as_ref()
                .is_some_and(|keys| keys.contains(&attnum))
    })
}

fn relation_column_index_by_name(
    relation: &crate::backend::parser::BoundRelation,
    column_name: &str,
) -> Result<usize, ExecError> {
    relation
        .desc
        .columns
        .iter()
        .enumerate()
        .find_map(|(index, column)| {
            (!column.dropped && column.name.eq_ignore_ascii_case(column_name)).then_some(index)
        })
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.to_string())))
}

fn not_null_constraint_for_column(
    catalog: &dyn CatalogLookup,
    relation: &crate::backend::parser::BoundRelation,
    column_name: &str,
) -> Result<Option<PgConstraintRow>, ExecError> {
    let column_index = relation_column_index_by_name(relation, column_name)?;
    let attnum = (column_index + 1) as i16;
    Ok(not_null_constraint_for_attnum(
        &catalog.constraint_rows_for_relation(relation.relation_oid),
        attnum,
    )
    .cloned())
}

fn relation_with_shape(
    shape_catalog: &dyn CatalogLookup,
    mut relation: crate::backend::parser::BoundRelation,
) -> crate::backend::parser::BoundRelation {
    if relation.desc.columns.is_empty()
        && let Some(shaped) = shape_catalog.lookup_relation_by_oid(relation.relation_oid)
    {
        relation.desc = shaped.desc;
    }
    relation
}

fn not_null_constraint_for_column_with_shape(
    catalog: &dyn CatalogLookup,
    shape_catalog: &dyn CatalogLookup,
    relation_oid: u32,
    column_name: &str,
) -> Result<Option<PgConstraintRow>, ExecError> {
    let Some(relation) = shape_catalog.lookup_relation_by_oid(relation_oid) else {
        return Ok(None);
    };
    let column_index = relation_column_index_by_name(&relation, column_name)?;
    let attnum = (column_index + 1) as i16;
    let rows = catalog.constraint_rows_for_relation(relation_oid);
    Ok(not_null_constraint_for_attnum(&rows, attnum).cloned())
}

fn inherited_not_null_parent_count(
    catalog: &dyn CatalogLookup,
    relation: &crate::backend::parser::BoundRelation,
    column_name: &str,
) -> Result<i16, ExecError> {
    let mut count = 0i16;
    for parent in catalog.inheritance_parents(relation.relation_oid) {
        let Some(parent_relation) = catalog.lookup_relation_by_oid(parent.inhparent) else {
            return Err(ExecError::Parse(ParseError::UnknownTable(
                parent.inhparent.to_string(),
            )));
        };
        match not_null_constraint_for_column(catalog, &parent_relation, column_name) {
            Ok(Some(row)) if !row.connoinherit => count = count.saturating_add(1),
            Ok(_) | Err(ExecError::Parse(ParseError::UnknownColumn(_))) => {}
            Err(err) => return Err(err),
        }
    }
    Ok(count)
}

fn inherited_not_null_parent_count_with_shape(
    catalog: &dyn CatalogLookup,
    shape_catalog: &dyn CatalogLookup,
    relation_oid: u32,
    column_name: &str,
) -> Result<i16, ExecError> {
    let mut count = 0i16;
    for parent in catalog.inheritance_parents(relation_oid) {
        match not_null_constraint_for_column_with_shape(
            catalog,
            shape_catalog,
            parent.inhparent,
            column_name,
        ) {
            Ok(Some(row)) if !row.connoinherit => count = count.saturating_add(1),
            Ok(_) | Err(ExecError::Parse(ParseError::UnknownColumn(_))) => {}
            Err(err) => return Err(err),
        }
    }
    Ok(count)
}

fn direct_inheritance_children(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Vec<crate::backend::parser::BoundRelation>, ExecError> {
    let mut children = catalog.inheritance_children(relation_oid);
    children.sort_by_key(|row| (row.inhseqno, row.inhrelid));
    children
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .map(|row| {
            catalog
                .lookup_relation_by_oid(row.inhrelid)
                .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(row.inhrelid.to_string())))
        })
        .collect()
}

fn collect_inheritance_descendant_oids(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    descendants: &mut Vec<u32>,
) {
    let mut children = catalog.inheritance_children(relation_oid);
    children.sort_by_key(|row| (row.inhseqno, row.inhrelid));
    for child in children.into_iter().filter(|row| !row.inhdetachpending) {
        descendants.push(child.inhrelid);
        collect_inheritance_descendant_oids(catalog, child.inhrelid, descendants);
    }
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

    #[allow(clippy::too_many_arguments)]
    fn alter_not_null_constraint_inheritability_in_transaction(
        &self,
        client_id: ClientId,
        relation: &crate::backend::parser::BoundRelation,
        relation_name: &str,
        row: &PgConstraintRow,
        inherit: bool,
        catalog: &dyn CatalogLookup,
        xid: TransactionId,
        cid: CommandId,
        interrupts: std::sync::Arc<crate::backend::utils::misc::interrupts::InterruptState>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let desired_no_inherit = !inherit;
        if row.connoinherit == desired_no_inherit {
            return Ok(());
        }
        if desired_no_inherit && row.coninhcount > 0 {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot alter inherited constraint \"{}\" on relation \"{}\"",
                    row.conname, relation_name
                ),
                detail: None,
                hint: None,
                sqlstate: "55000",
            });
        }

        let attnum = *attnums_from_constraint(row)?
            .first()
            .expect("not null attnum");
        let column_index = column_index_for_attnum(relation, attnum)?;
        let column_name = relation.desc.columns[column_index].name.clone();
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: std::sync::Arc::clone(&interrupts),
        };
        let effect = self
            .catalog
            .write()
            .alter_not_null_constraint_state_by_attnum_mvcc(
                relation.relation_oid,
                attnum,
                row.oid,
                &row.conname,
                None,
                Some(desired_no_inherit),
                None,
                None,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);

        if inherit {
            let action = crate::backend::parser::NotNullConstraintAction {
                constraint_name: row.conname.clone(),
                column: column_name,
                not_valid: !row.convalidated,
                no_inherit: false,
                primary_key_owned: row.conperiod,
                is_local: true,
                inhcount: 0,
            };
            self.propagate_not_null_constraint_to_inheritors(
                client_id,
                xid,
                cid.saturating_add(1),
                relation,
                &action,
                false,
                None,
                catalog_effects,
            )?;
            return Ok(());
        }

        for child in direct_inheritance_children(catalog, relation.relation_oid)? {
            let child_column_index = relation_column_index_by_name(&child, &column_name)?;
            let child_row = not_null_constraint_for_column(catalog, &child, &column_name)?;
            let Some(child_row) = child_row else {
                continue;
            };
            let child_attnum = (child_column_index + 1) as i16;
            let new_inhcount = child_row.coninhcount.saturating_sub(1);
            let effect = self
                .catalog
                .write()
                .alter_not_null_constraint_state_by_attnum_mvcc(
                    child.relation_oid,
                    child_attnum,
                    child_row.oid,
                    &child_row.conname,
                    None,
                    None,
                    Some(true),
                    Some(new_inhcount),
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }

        Ok(())
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
            .ok_or_else(|| ExecError::DetailedError {
                message: format!(
                    "constraint \"{}\" of relation \"{}\" does not exist",
                    alter_stmt.constraint_name,
                    relation_basename(&alter_stmt.table_name)
                ),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        if alter_stmt.not_valid {
            return Err(ExecError::DetailedError {
                message: "constraints cannot be altered to be NOT VALID".into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
        if let Some(inherit) = alter_stmt.inheritability {
            if row.contype != CONSTRAINT_NOTNULL {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "constraint \"{}\" of relation \"{}\" is not a not-null constraint",
                        alter_stmt.constraint_name,
                        relation_basename(&alter_stmt.table_name)
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42809",
                });
            }
            self.alter_not_null_constraint_inheritability_in_transaction(
                client_id,
                &relation,
                relation_basename(&alter_stmt.table_name),
                &row,
                inherit,
                &catalog,
                xid,
                cid,
                std::sync::Arc::clone(&interrupts),
                catalog_effects,
            )?;
            if alter_stmt.deferrable.is_none()
                && alter_stmt.initially_deferred.is_none()
                && alter_stmt.enforced.is_none()
            {
                return Ok(StatementResult::AffectedRows(0));
            }
        }
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
        if row.conparentid != 0 || row.coninhcount > 0 {
            return Err(inherited_foreign_key_alter_error(
                &row,
                relation_basename(&alter_stmt.table_name),
                &catalog,
            ));
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
                &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
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
        if row.conenforced && !enforced {
            self.drop_foreign_key_triggers_in_transaction(
                client_id,
                xid,
                cid,
                &row,
                &catalog,
                catalog_effects,
            )?;
        }
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
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        let mut updated_row = row.clone();
        updated_row.condeferrable = deferrable;
        updated_row.condeferred = initially_deferred;
        updated_row.conenforced = enforced;
        updated_row.convalidated = validated;
        if !row.conenforced && enforced {
            self.create_foreign_key_triggers_in_transaction(
                client_id,
                xid,
                cid.saturating_add(catalog_effects.len() as u32),
                &updated_row,
                catalog_effects,
            )?;
        } else if row.conenforced && enforced {
            self.alter_foreign_key_trigger_deferrability_in_transaction(
                client_id,
                xid,
                cid.saturating_add(catalog_effects.len() as u32),
                &updated_row,
                &catalog,
                catalog_effects,
            )?;
        }
        self.alter_partition_child_foreign_key_constraints_in_transaction(
            client_id,
            xid,
            cid.saturating_add(catalog_effects.len() as u32),
            &row,
            deferrable,
            initially_deferred,
            enforced,
            validated,
            &catalog,
            catalog_effects,
        )?;
        Ok(StatementResult::AffectedRows(0))
    }

    #[allow(clippy::too_many_arguments)]
    fn alter_partition_child_foreign_key_constraints_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent_constraint: &PgConstraintRow,
        deferrable: bool,
        initially_deferred: bool,
        enforced: bool,
        validated: bool,
        catalog: &dyn CatalogLookup,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let all_rows = catalog.constraint_rows();
        let mut pending = all_rows
            .iter()
            .filter(|row| {
                row.contype == CONSTRAINT_FOREIGN && row.conparentid == parent_constraint.oid
            })
            .cloned()
            .map(|row| (parent_constraint.clone(), row))
            .collect::<Vec<_>>();
        let interrupts = self.interrupt_state(client_id);
        let mut next_cid = cid;
        while let Some((parent_row, child_row)) = pending.pop() {
            let child_relation = relation_by_oid_for_constraint_command(
                self,
                client_id,
                catalog,
                child_row.conrelid,
            )
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnknownTable(child_row.conrelid.to_string()))
            })?;
            let referenced_side = parent_row.conrelid == child_row.conrelid;
            if child_row.conenforced && !enforced {
                self.drop_foreign_key_triggers_in_transaction(
                    client_id,
                    xid,
                    next_cid,
                    &child_row,
                    catalog,
                    catalog_effects,
                )?;
                next_cid = next_cid.saturating_add(1);
            }
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: next_cid,
                client_id,
                waiter: None,
                interrupts: std::sync::Arc::clone(&interrupts),
            };
            next_cid = next_cid.saturating_add(1);
            let child_validated = if !enforced {
                false
            } else if !child_row.conenforced {
                true
            } else {
                validated
            };
            let effect = self
                .catalog
                .write()
                .alter_foreign_key_constraint_attributes_mvcc(
                    child_relation.relation_oid,
                    &child_row.conname,
                    deferrable,
                    initially_deferred,
                    enforced,
                    child_validated,
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            let mut updated_child = child_row.clone();
            updated_child.condeferrable = deferrable;
            updated_child.condeferred = initially_deferred;
            updated_child.conenforced = enforced;
            updated_child.convalidated = child_validated;
            if !child_row.conenforced && enforced {
                next_cid = if referenced_side {
                    self.create_foreign_key_action_triggers_in_transaction(
                        client_id,
                        xid,
                        next_cid,
                        &updated_child,
                        catalog_effects,
                    )?
                } else {
                    self.create_foreign_key_check_triggers_in_transaction(
                        client_id,
                        xid,
                        next_cid,
                        &updated_child,
                        catalog_effects,
                    )?
                };
            } else if child_row.conenforced && enforced {
                self.alter_foreign_key_trigger_deferrability_in_transaction(
                    client_id,
                    xid,
                    next_cid,
                    &updated_child,
                    catalog,
                    catalog_effects,
                )?;
                next_cid = next_cid.saturating_add(1);
            }
            pending.extend(
                all_rows
                    .iter()
                    .filter(|row| {
                        row.contype == CONSTRAINT_FOREIGN && row.conparentid == child_row.oid
                    })
                    .cloned()
                    .map(|row| (updated_child.clone(), row)),
            );
        }
        Ok(next_cid)
    }

    pub(super) fn drop_partition_child_foreign_key_constraints_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent_constraint: &PgConstraintRow,
        catalog: &dyn CatalogLookup,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let all_rows = catalog.constraint_rows();
        let mut pending = all_rows
            .iter()
            .filter(|row| {
                row.contype == CONSTRAINT_FOREIGN && row.conparentid == parent_constraint.oid
            })
            .cloned()
            .collect::<Vec<_>>();
        let interrupts = self.interrupt_state(client_id);
        let mut next_cid = cid;
        while let Some(child_row) = pending.pop() {
            pending.extend(
                all_rows
                    .iter()
                    .filter(|row| {
                        row.contype == CONSTRAINT_FOREIGN && row.conparentid == child_row.oid
                    })
                    .cloned(),
            );
            self.drop_foreign_key_triggers_in_transaction(
                client_id,
                xid,
                next_cid,
                &child_row,
                catalog,
                catalog_effects,
            )?;
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: next_cid.saturating_add(catalog_effects.len() as u32),
                client_id,
                waiter: None,
                interrupts: std::sync::Arc::clone(&interrupts),
            };
            let (_removed, effect) = self
                .catalog
                .write()
                .drop_relation_constraint_mvcc(child_row.conrelid, &child_row.conname, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            next_cid = next_cid.saturating_add(1);
        }
        Ok(next_cid)
    }

    pub(super) fn drop_partition_child_index_backed_constraints_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent_constraint: &PgConstraintRow,
        catalog: &dyn CatalogLookup,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        fn collect_descendants(
            parent_constraint_oid: u32,
            all_rows: &[PgConstraintRow],
            out: &mut Vec<PgConstraintRow>,
        ) {
            for child_row in all_rows.iter().filter(|row| {
                matches!(
                    row.contype,
                    CONSTRAINT_PRIMARY | CONSTRAINT_UNIQUE | CONSTRAINT_EXCLUSION
                ) && row.conparentid == parent_constraint_oid
            }) {
                collect_descendants(child_row.oid, all_rows, out);
                out.push(child_row.clone());
            }
        }

        let all_rows = catalog.constraint_rows();
        let mut rows_to_drop = Vec::new();
        collect_descendants(parent_constraint.oid, &all_rows, &mut rows_to_drop);
        let interrupts = self.interrupt_state(client_id);
        let mut next_cid = cid;
        for child_row in rows_to_drop {
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
                    child_row.conrelid,
                    &child_row.conname,
                    &constraint_ctx,
                )
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
            next_cid = next_cid.saturating_add(1);

            if removed.conindid != 0 {
                let drop_index_ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: next_cid,
                    client_id,
                    waiter: None,
                    interrupts: std::sync::Arc::clone(&interrupts),
                };
                let (_entry, effect) = self
                    .catalog
                    .write()
                    .drop_relation_entry_by_oid_mvcc(removed.conindid, &drop_index_ctx)
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
                next_cid = next_cid.saturating_add(1);
            }
        }
        Ok(next_cid)
    }

    pub(super) fn detach_partition_child_foreign_key_constraints_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent_oid: u32,
        child_oid: u32,
        catalog: &dyn CatalogLookup,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let inherited_constraints = catalog
            .constraint_rows_for_relation(child_oid)
            .into_iter()
            .filter(|row| {
                row.contype == CONSTRAINT_FOREIGN
                    && row.conparentid != 0
                    && catalog
                        .constraint_row_by_oid(row.conparentid)
                        .is_some_and(|parent| parent.conrelid == parent_oid)
            })
            .collect::<Vec<_>>();
        if inherited_constraints.is_empty() {
            return Ok(cid);
        }
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        for row in inherited_constraints {
            let effect = self
                .catalog
                .write()
                .update_foreign_key_constraint_inheritance_mvcc(
                    child_oid, row.oid, 0, true, 0, &ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        Ok(cid.saturating_add(1))
    }

    pub(crate) fn execute_alter_table_rename_constraint_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableRenameConstraintStatement,
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
        let rows = catalog.constraint_rows_for_relation(relation.relation_oid);
        let lock_descendants = !alter_stmt.only
            && find_constraint_row(&rows, &alter_stmt.constraint_name).is_some_and(|row| {
                matches!(row.contype, CONSTRAINT_CHECK | CONSTRAINT_NOTNULL) && !row.connoinherit
            });
        let lock_queue = build_alter_table_work_queue(&catalog, &relation, !lock_descendants)?;
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
        for rel in locked_rels {
            self.table_locks.unlock_table(rel, client_id);
        }
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
        let constraint =
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
        let propagates = matches!(constraint.contype, CONSTRAINT_CHECK | CONSTRAINT_NOTNULL)
            && !constraint.connoinherit;
        if propagates
            && alter_stmt.only
            && has_inheritance_children(&catalog, relation.relation_oid)
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "inherited constraint \"{}\" must be renamed in child tables too",
                    alter_stmt.constraint_name
                ),
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
            interrupts,
        };
        let work_queue = if propagates {
            build_alter_table_work_queue(&catalog, &relation, alter_stmt.only)?
        } else {
            build_alter_table_work_queue(&catalog, &relation, true)?
        };
        for item in work_queue.into_iter().rev() {
            let rows = catalog.constraint_rows_for_relation(item.relation.relation_oid);
            let row = find_constraint_row(&rows, &alter_stmt.constraint_name).ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "existing table constraint",
                    actual: format!(
                        "constraint \"{}\" does not exist",
                        alter_stmt.constraint_name
                    ),
                })
            })?;
            if row.coninhcount > item.expected_parents {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "cannot rename inherited constraint \"{}\"",
                        alter_stmt.constraint_name
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42P16",
                });
            }
            if find_constraint_row(&rows, &new_constraint_name).is_some() {
                return Err(ExecError::Parse(ParseError::TableAlreadyExists(
                    new_constraint_name.clone(),
                )));
            }
            let effect = self
                .catalog
                .write()
                .rename_relation_constraint_mvcc(
                    item.relation.relation_oid,
                    &alter_stmt.constraint_name,
                    &alter_stmt.new_constraint_name,
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_add_constraint_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAddConstraintStatement,
        configured_search_path: Option<&[String]>,
        datetime_config: Option<&crate::backend::utils::misc::guc_datetime::DateTimeConfig>,
        gucs: Option<&std::collections::HashMap<String, String>>,
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
            gucs,
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
        gucs: Option<&std::collections::HashMap<String, String>>,
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
            crate::backend::parser::NormalizedAlterTableConstraint::Noop => {}
            crate::backend::parser::NormalizedAlterTableConstraint::NotNull(action) => {
                if relation.relkind == 'p' && action.no_inherit {
                    return Err(ExecError::DetailedError {
                        message: "not-null constraints on partitioned tables cannot be NO INHERIT"
                            .into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42P16",
                    });
                }
                if alter_stmt.only
                    && !action.no_inherit
                    && !direct_inheritance_children(&catalog, relation.relation_oid)?.is_empty()
                {
                    return Err(ExecError::DetailedError {
                        message: "constraint must be added to child tables too".into(),
                        detail: None,
                        hint: (relation.relkind == 'p')
                            .then_some("Do not specify the ONLY keyword.".into()),
                        sqlstate: "42P16",
                    });
                }
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
                        action.constraint_name.clone(),
                        !action.not_valid,
                        action.no_inherit,
                        false,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                let (_constraint_oid, effect) = effect;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                if !alter_stmt.only && !action.no_inherit {
                    self.propagate_not_null_constraint_to_inheritors(
                        client_id,
                        xid,
                        cid.saturating_add(1),
                        &relation,
                        &action,
                        true,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
            }
            crate::backend::parser::NormalizedAlterTableConstraint::Check(action) => {
                if relation.relkind == 'p' && action.no_inherit {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "cannot add NO INHERIT constraint to partitioned table \"{}\"",
                            table_name
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42P16",
                    });
                }
                if alter_stmt.only
                    && !action.no_inherit
                    && !direct_inheritance_children(&catalog, relation.relation_oid)?.is_empty()
                {
                    return Err(ExecError::DetailedError {
                        message: "constraint must be added to child tables too".into(),
                        detail: None,
                        hint: (relation.relkind == 'p')
                            .then_some("Do not specify the ONLY keyword.".into()),
                        sqlstate: "42P16",
                    });
                }
                crate::backend::parser::bind_check_constraint_expr(
                    &action.expr_sql,
                    Some(&table_name),
                    &relation.desc,
                    &catalog,
                )
                .map_err(ExecError::Parse)?;
                if action.enforced && !action.not_valid {
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
                let existing = existing_constraints.iter().find(|row| {
                    row.contype == CONSTRAINT_CHECK
                        && row.conname.eq_ignore_ascii_case(&action.constraint_name)
                });
                let parent_constraint = if let Some(existing) = existing {
                    if !check_constraint_exprs_match(existing, &action.expr_sql) {
                        return Err(ExecError::Parse(ParseError::InvalidTableDefinition(
                            format!(
                                "constraint \"{}\" conflicts with inherited constraint",
                                action.constraint_name
                            ),
                        )));
                    }
                    if existing.conenforced && !action.enforced {
                        return Err(ExecError::DetailedError {
                            message: format!(
                                "constraint \"{}\" conflicts with NOT ENFORCED constraint on relation \"{}\"",
                                action.constraint_name, table_name
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42P17",
                        });
                    }
                    push_notice(format!(
                        "merging constraint \"{}\" with inherited definition",
                        action.constraint_name
                    ));
                    let conenforced = existing.conenforced || action.enforced;
                    let convalidated = conenforced && (existing.convalidated || action.enforced);
                    let effect = self
                        .catalog
                        .write()
                        .update_check_constraint_state_mvcc(
                            relation.relation_oid,
                            existing.oid,
                            Some(conenforced),
                            Some(convalidated),
                            action.parent_constraint_oid.or(Some(existing.conparentid)),
                            Some(true),
                            Some(existing.coninhcount.max(action.inhcount)),
                            Some(false),
                            &ctx,
                        )
                        .map_err(map_catalog_error)?;
                    self.apply_catalog_mutation_effect_immediate(&effect)?;
                    catalog_effects.push(effect);
                    existing.clone()
                } else {
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
                    parent_constraint
                };
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
                        !alter_stmt.only,
                        configured_search_path,
                        None,
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
                            self.apply_catalog_mutation_effect_immediate(&effect)?;
                            primary_key_owned_not_null_oids.push(not_null_oid);
                            catalog_effects.push(effect);
                            if !alter_stmt.only {
                                let not_null_action =
                                    crate::backend::parser::NotNullConstraintAction {
                                        constraint_name: not_null_name,
                                        column: relation.desc.columns[column_index].name.clone(),
                                        not_valid: false,
                                        no_inherit: false,
                                        primary_key_owned: true,
                                        is_local: true,
                                        inhcount: 0,
                                    };
                                self.propagate_not_null_constraint_to_inheritors(
                                    client_id,
                                    xid,
                                    cid.saturating_add(1),
                                    &relation,
                                    &not_null_action,
                                    true,
                                    configured_search_path,
                                    catalog_effects,
                                )?;
                            }
                        } else if let Some(row) = not_null_constraint_for_attnum(
                            &existing_constraints,
                            (column_index + 1) as i16,
                        ) {
                            verify_not_null_pk_compatible(
                                row,
                                &relation.desc.columns[column_index].name,
                                &table_name,
                            )?;
                            if !alter_stmt.only {
                                let not_null_action =
                                    crate::backend::parser::NotNullConstraintAction {
                                        constraint_name: row.conname.clone(),
                                        column: relation.desc.columns[column_index].name.clone(),
                                        not_valid: !row.convalidated,
                                        no_inherit: false,
                                        primary_key_owned: false,
                                        is_local: row.conislocal,
                                        inhcount: row.coninhcount,
                                    };
                                self.propagate_not_null_constraint_to_inheritors(
                                    client_id,
                                    xid,
                                    cid.saturating_add(1),
                                    &relation,
                                    &not_null_action,
                                    true,
                                    configured_search_path,
                                    catalog_effects,
                                )?;
                            }
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
                let index_columns =
                    constraint_index_columns_with_expr_types(&action, &relation, &catalog)?;
                let mut storage_columns = index_columns.clone();
                storage_columns.extend(
                    action
                        .include_columns
                        .iter()
                        .cloned()
                        .map(crate::backend::parser::IndexColumnDef::from),
                );
                let exclusion_operator_oids = if action.exclusion {
                    Some(self.exclusion_constraint_operator_oids_for_index_columns(
                        &relation.desc,
                        &index_columns,
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
                if let Some(operator_oids) = exclusion_operator_oids.clone()
                    && index_columns.iter().all(|column| column.expr_sql.is_none())
                {
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
                } else if action.exclusion {
                    // :HACK: Expression exclusion constraints are needed for PostgreSQL
                    // compatibility in inherit regression DDL. The existing exclusion
                    // validator is column-oriented, so column exclusions keep full
                    // validation while expression enforcement is deferred.
                }
                let (access_method_oid, access_method_handler, build_options) = if action.exclusion
                {
                    self.resolve_exclusion_index_build_options(
                        client_id,
                        Some((xid, index_cid)),
                        action.access_method.as_deref().unwrap_or("gist"),
                        &relation,
                        &index_columns,
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
                    Some(crate::backend::executor::executor_catalog(catalog.clone())),
                    &storage_columns,
                    None,
                    !action.exclusion,
                    action.primary,
                    action.nulls_not_distinct,
                    if relation.relpersistence == 't' {
                        None
                    } else {
                        Some(super::tablespace::resolve_relation_tablespace_oid(
                            self,
                            client_id,
                            Some((xid, index_cid)),
                            action.tablespace.as_deref(),
                            gucs,
                        )?)
                    },
                    xid,
                    index_cid,
                    access_method_oid,
                    access_method_handler,
                    &build_options,
                    65_536,
                    false,
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
                if alter_stmt.only && relation.relkind == 'p' {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "cannot use ONLY for foreign key on partitioned table \"{}\" referencing relation \"{}\"",
                            relation_basename(&alter_stmt.table_name),
                            action.referenced_table
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42809",
                    });
                }
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
                            datetime_config,
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
                        action.constraint_name.clone(),
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
                        0,
                        true,
                        0,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
                if action.enforced {
                    self.create_foreign_key_triggers_in_transaction(
                        client_id,
                        xid,
                        cid.saturating_add(1),
                        &constraint_row,
                        catalog_effects,
                    )?;
                }
                let mut next_cid = cid.saturating_add(catalog_effects.len() as u32);
                if relation.relkind == 'p' {
                    next_cid = self.create_partition_child_foreign_key_constraints_in_transaction(
                        client_id,
                        xid,
                        next_cid,
                        &relation,
                        &constraint_row,
                        &action,
                        &local_attnums,
                        &referenced_attnums,
                        delete_set_attnums.as_deref(),
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
                if referenced_relation.relkind == 'p' {
                    self.create_referenced_partition_foreign_key_constraints_in_transaction(
                        client_id,
                        xid,
                        next_cid,
                        &referenced_relation,
                        &constraint_row,
                        &referenced_attnums,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
            }
        }

        Ok(StatementResult::AffectedRows(0))
    }

    #[allow(clippy::too_many_arguments)]
    fn create_partition_child_foreign_key_constraints_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent: &crate::backend::parser::BoundRelation,
        parent_constraint: &PgConstraintRow,
        action: &ForeignKeyConstraintAction,
        parent_attnums: &[i16],
        referenced_attnums: &[i16],
        delete_set_attnums: Option<&[i16]>,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let children = partition_descendants(&catalog, parent.relation_oid)?;
        let interrupts = self.interrupt_state(client_id);
        let mut next_cid = cid;
        for child in children {
            let local_attnums =
                attnums_by_parent_column_names(&parent.desc, &child.desc, parent_attnums)?;
            let child_delete_set_attnums = delete_set_attnums
                .map(|attnums| attnums_by_parent_column_names(&parent.desc, &child.desc, attnums))
                .transpose()?;
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: next_cid,
                client_id,
                waiter: None,
                interrupts: std::sync::Arc::clone(&interrupts),
            };
            next_cid = next_cid.saturating_add(1);
            let mut existing = None;
            let mut already_inherited = None;
            for row in catalog.constraint_rows_for_relation(child.relation_oid) {
                if !foreign_key_attach_key_matches(
                    &row,
                    parent_constraint,
                    &local_attnums,
                    referenced_attnums,
                    child_delete_set_attnums.as_deref(),
                ) {
                    continue;
                }
                if row.conenforced != parent_constraint.conenforced {
                    let relation_name = catalog
                        .class_row_by_oid(child.relation_oid)
                        .map(|class| class.relname)
                        .unwrap_or_else(|| child.relation_oid.to_string());
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "constraint \"{}\" enforceability conflicts with constraint \"{}\" on relation \"{}\"",
                            parent_constraint.conname, row.conname, relation_name
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42P16",
                    });
                }
                if !foreign_key_attach_attributes_match(&row, parent_constraint) {
                    continue;
                }
                if row.conparentid == parent_constraint.oid {
                    already_inherited = Some(row);
                    break;
                }
                if row.conparentid == 0 {
                    existing = Some(row);
                    break;
                }
            }
            let constraint_row = if let Some(row) = already_inherited {
                row
            } else if let Some(existing) = existing {
                let child_validated =
                    existing.convalidated || (parent_constraint.convalidated && action.enforced);
                if parent_constraint.convalidated && !existing.convalidated && action.enforced {
                    validate_attached_foreign_key_rows_if_needed(
                        self,
                        &child,
                        &existing,
                        &local_attnums,
                        referenced_attnums,
                        &catalog,
                        client_id,
                        xid,
                        next_cid,
                        std::sync::Arc::clone(&interrupts),
                    )?;
                }
                let effect = self
                    .catalog
                    .write()
                    .update_foreign_key_constraint_inheritance_mvcc(
                        child.relation_oid,
                        existing.oid,
                        parent_constraint.oid,
                        false,
                        existing.coninhcount.saturating_add(1),
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                let mut row = existing.clone();
                row.conparentid = parent_constraint.oid;
                row.conislocal = false;
                row.coninhcount = row.coninhcount.saturating_add(1);
                if row.convalidated != child_validated {
                    let attr_ctx = CatalogWriteContext {
                        pool: self.pool.clone(),
                        txns: self.txns.clone(),
                        xid,
                        cid: next_cid,
                        client_id,
                        waiter: None,
                        interrupts: std::sync::Arc::clone(&interrupts),
                    };
                    next_cid = next_cid.saturating_add(1);
                    let effect = self
                        .catalog
                        .write()
                        .alter_foreign_key_constraint_attributes_mvcc(
                            child.relation_oid,
                            &row.conname,
                            row.condeferrable,
                            row.condeferred,
                            row.conenforced,
                            child_validated,
                            &attr_ctx,
                        )
                        .map_err(map_catalog_error)?;
                    self.apply_catalog_mutation_effect_immediate(&effect)?;
                    catalog_effects.push(effect);
                    row.convalidated = child_validated;
                }
                row
            } else {
                let (constraint_row, effect) = self
                    .catalog
                    .write()
                    .create_foreign_key_constraint_mvcc(
                        child.relation_oid,
                        action.constraint_name.clone(),
                        action.deferrable,
                        action.initially_deferred,
                        action.enforced,
                        action.enforced && !action.not_valid,
                        &local_attnums,
                        action.referenced_relation_oid,
                        action.referenced_index_oid,
                        referenced_attnums,
                        foreign_key_action_code(action.on_update),
                        foreign_key_action_code(action.on_delete),
                        foreign_key_match_code(action.match_type),
                        child_delete_set_attnums.as_deref(),
                        action.period.is_some(),
                        parent_constraint.oid,
                        false,
                        1,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                constraint_row
            };
            if action.enforced
                && catalog
                    .trigger_rows_for_relation(child.relation_oid)
                    .into_iter()
                    .all(|row| row.tgconstraint != constraint_row.oid)
            {
                next_cid = self.create_foreign_key_check_triggers_in_transaction(
                    client_id,
                    xid,
                    next_cid,
                    &constraint_row,
                    catalog_effects,
                )?;
            }
        }
        Ok(next_cid)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn create_referenced_partition_foreign_key_constraints_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        referenced_parent: &crate::backend::parser::BoundRelation,
        parent_constraint: &PgConstraintRow,
        referenced_attnums: &[i16],
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let mut used_names = catalog
            .constraint_rows_for_relation(parent_constraint.conrelid)
            .into_iter()
            .map(|row| row.conname.to_ascii_lowercase())
            .collect::<std::collections::BTreeSet<_>>();
        self.create_referenced_partition_foreign_key_constraint_descendants_in_transaction(
            client_id,
            xid,
            cid,
            referenced_parent,
            parent_constraint,
            referenced_attnums,
            &parent_constraint.conname,
            &mut used_names,
            configured_search_path,
            catalog_effects,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_referenced_partition_foreign_key_constraint_descendants_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        referenced_parent: &crate::backend::parser::BoundRelation,
        parent_constraint: &PgConstraintRow,
        parent_referenced_attnums: &[i16],
        clone_name_base: &str,
        used_names: &mut std::collections::BTreeSet<String>,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let mut next_cid = cid;
        for child_relation in
            sorted_partition_child_relations(&catalog, referenced_parent.relation_oid)?
        {
            next_cid = self
                .create_referenced_partition_foreign_key_constraint_for_partition_in_transaction(
                    client_id,
                    xid,
                    next_cid,
                    referenced_parent,
                    &child_relation,
                    parent_constraint,
                    parent_referenced_attnums,
                    clone_name_base,
                    used_names,
                    configured_search_path,
                    catalog_effects,
                )?;
        }
        Ok(next_cid)
    }

    #[allow(clippy::too_many_arguments)]
    fn create_referenced_partition_foreign_key_constraint_for_partition_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        referenced_parent: &crate::backend::parser::BoundRelation,
        referenced_child: &crate::backend::parser::BoundRelation,
        parent_constraint: &PgConstraintRow,
        parent_referenced_attnums: &[i16],
        clone_name_base: &str,
        used_names: &mut std::collections::BTreeSet<String>,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let referenced_attnums = attnums_by_parent_column_names(
            &referenced_parent.desc,
            &referenced_child.desc,
            parent_referenced_attnums,
        )?;
        let referenced_index = find_referenced_foreign_key_index(
            &catalog,
            referenced_child.relation_oid,
            &referenced_attnums,
            parent_constraint.conperiod,
        )
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "referenced UNIQUE or PRIMARY KEY index",
                actual: format!(
                    "missing referenced index for partition {}",
                    catalog
                        .class_row_by_oid(referenced_child.relation_oid)
                        .map(|row| row.relname)
                        .unwrap_or_else(|| referenced_child.relation_oid.to_string())
                ),
            })
        })?;
        let local_attnums = parent_constraint.conkey.clone().ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "foreign key columns",
                actual: format!("missing conkey for {}", parent_constraint.conname),
            })
        })?;
        let constraint_name = choose_partition_clone_constraint_name(clone_name_base, used_names);
        let interrupts = self.interrupt_state(client_id);
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
                parent_constraint.conrelid,
                constraint_name,
                parent_constraint.condeferrable,
                parent_constraint.condeferred,
                parent_constraint.conenforced,
                parent_constraint.convalidated,
                &local_attnums,
                referenced_child.relation_oid,
                referenced_index.relation_oid,
                &referenced_attnums,
                parent_constraint.confupdtype,
                parent_constraint.confdeltype,
                parent_constraint.confmatchtype,
                parent_constraint.confdelsetcols.as_deref(),
                parent_constraint.conperiod,
                parent_constraint.oid,
                false,
                1,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        let mut next_cid = cid.saturating_add(1);
        if parent_constraint.conenforced {
            next_cid = self.create_foreign_key_action_triggers_in_transaction(
                client_id,
                xid,
                next_cid,
                &constraint_row,
                catalog_effects,
            )?;
        }
        self.create_referenced_partition_foreign_key_constraint_descendants_in_transaction(
            client_id,
            xid,
            next_cid,
            referenced_child,
            &constraint_row,
            &referenced_attnums,
            clone_name_base,
            used_names,
            configured_search_path,
            catalog_effects,
        )
    }

    pub(super) fn reconcile_partitioned_parent_foreign_keys_for_attached_child_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent_oid: u32,
        child_oid: u32,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let latest_catalog = self.lazy_catalog_lookup(
            client_id,
            Some((xid, CommandId::MAX)),
            configured_search_path,
        );
        let parent = catalog
            .relation_by_oid(parent_oid)
            .or_else(|| catalog.lookup_relation_by_oid(parent_oid))
            .or_else(|| latest_catalog.relation_by_oid(parent_oid))
            .or_else(|| latest_catalog.lookup_relation_by_oid(parent_oid))
            .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(parent_oid.to_string())))?;
        let child_from_primary_catalog = catalog
            .relation_by_oid(child_oid)
            .or_else(|| catalog.lookup_relation_by_oid(child_oid));
        let child_visible_in_primary_catalog = child_from_primary_catalog.is_some();
        let child = child_from_primary_catalog
            .or_else(|| latest_catalog.relation_by_oid(child_oid))
            .or_else(|| latest_catalog.lookup_relation_by_oid(child_oid))
            .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(child_oid.to_string())))?;
        let mut target_relations = vec![child];
        if child_visible_in_primary_catalog {
            target_relations.extend(partition_descendants(&catalog, child_oid)?);
        }
        let parent_constraints = catalog
            .constraint_rows_for_relation(parent_oid)
            .into_iter()
            .filter(|row| {
                row.contype == CONSTRAINT_FOREIGN
                    && !is_referenced_side_foreign_key_clone(row, &catalog)
            })
            .collect::<Vec<_>>();
        let interrupts = self.interrupt_state(client_id);
        let mut next_cid = cid;
        for parent_constraint in parent_constraints {
            let parent_attnums = parent_constraint.conkey.clone().ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "foreign key columns",
                    actual: format!("missing conkey for {}", parent_constraint.conname),
                })
            })?;
            let referenced_attnums = parent_constraint.confkey.clone().ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "referenced foreign key columns",
                    actual: format!("missing confkey for {}", parent_constraint.conname),
                })
            })?;
            for target in &target_relations {
                let local_attnums =
                    attnums_by_parent_column_names(&parent.desc, &target.desc, &parent_attnums)?;
                let delete_set_attnums = parent_constraint
                    .confdelsetcols
                    .as_deref()
                    .map(|attnums| {
                        attnums_by_parent_column_names(&parent.desc, &target.desc, attnums)
                    })
                    .transpose()?;
                let mut existing = None;
                let mut already_inherited = None;
                for row in catalog.constraint_rows_for_relation(target.relation_oid) {
                    if !foreign_key_attach_key_matches(
                        &row,
                        &parent_constraint,
                        &local_attnums,
                        &referenced_attnums,
                        delete_set_attnums.as_deref(),
                    ) {
                        continue;
                    }
                    if row.conenforced != parent_constraint.conenforced {
                        let relation_name = catalog
                            .class_row_by_oid(target.relation_oid)
                            .map(|class| class.relname)
                            .unwrap_or_else(|| target.relation_oid.to_string());
                        return Err(ExecError::DetailedError {
                            message: format!(
                                "constraint \"{}\" enforceability conflicts with constraint \"{}\" on relation \"{}\"",
                                parent_constraint.conname, row.conname, relation_name
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42P16",
                        });
                    }
                    if foreign_key_attach_attributes_match(&row, &parent_constraint) {
                        if row.conparentid == 0 {
                            existing = Some(row);
                        } else {
                            already_inherited = Some(row);
                        }
                        break;
                    }
                }
                let target_visible_for_write = catalog
                    .relation_by_oid(target.relation_oid)
                    .or_else(|| catalog.lookup_relation_by_oid(target.relation_oid))
                    .is_some();
                if !target_visible_for_write {
                    continue;
                }
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: next_cid,
                    client_id,
                    waiter: None,
                    interrupts: std::sync::Arc::clone(&interrupts),
                };
                next_cid = next_cid.saturating_add(1);
                let constraint_row = if let Some(mut row) = already_inherited {
                    let child_validated =
                        row.convalidated || (parent_constraint.convalidated && row.conenforced);
                    if parent_constraint.convalidated && !row.convalidated {
                        validate_attached_foreign_key_rows_if_needed(
                            self,
                            target,
                            &row,
                            &local_attnums,
                            &referenced_attnums,
                            &catalog,
                            client_id,
                            xid,
                            next_cid,
                            std::sync::Arc::clone(&interrupts),
                        )?;
                    }
                    if row.convalidated != child_validated {
                        let effect = self
                            .catalog
                            .write()
                            .alter_foreign_key_constraint_attributes_mvcc(
                                target.relation_oid,
                                &row.conname,
                                row.condeferrable,
                                row.condeferred,
                                row.conenforced,
                                child_validated,
                                &ctx,
                            )
                            .map_err(map_catalog_error)?;
                        self.apply_catalog_mutation_effect_immediate(&effect)?;
                        catalog_effects.push(effect);
                        row.convalidated = child_validated;
                    }
                    row
                } else if let Some(existing) = existing {
                    let child_validated = existing.convalidated
                        || (parent_constraint.convalidated && parent_constraint.conenforced);
                    if parent_constraint.convalidated && !existing.convalidated {
                        validate_attached_foreign_key_rows_if_needed(
                            self,
                            target,
                            &existing,
                            &local_attnums,
                            &referenced_attnums,
                            &catalog,
                            client_id,
                            xid,
                            next_cid,
                            std::sync::Arc::clone(&interrupts),
                        )?;
                    }
                    let effect = self
                        .catalog
                        .write()
                        .update_foreign_key_constraint_inheritance_mvcc(
                            target.relation_oid,
                            existing.oid,
                            parent_constraint.oid,
                            existing.conislocal,
                            existing.coninhcount.saturating_add(1),
                            &ctx,
                        )
                        .map_err(map_catalog_error)?;
                    self.apply_catalog_mutation_effect_immediate(&effect)?;
                    catalog_effects.push(effect);
                    let mut row = existing.clone();
                    row.conparentid = parent_constraint.oid;
                    row.coninhcount = row.coninhcount.saturating_add(1);
                    if row.convalidated != child_validated {
                        let attr_ctx = CatalogWriteContext {
                            pool: self.pool.clone(),
                            txns: self.txns.clone(),
                            xid,
                            cid: next_cid,
                            client_id,
                            waiter: None,
                            interrupts: std::sync::Arc::clone(&interrupts),
                        };
                        next_cid = next_cid.saturating_add(1);
                        let effect = self
                            .catalog
                            .write()
                            .alter_foreign_key_constraint_attributes_mvcc(
                                target.relation_oid,
                                &row.conname,
                                row.condeferrable,
                                row.condeferred,
                                row.conenforced,
                                child_validated,
                                &attr_ctx,
                            )
                            .map_err(map_catalog_error)?;
                        self.apply_catalog_mutation_effect_immediate(&effect)?;
                        catalog_effects.push(effect);
                        row.convalidated = child_validated;
                    }
                    row
                } else {
                    if parent_constraint.convalidated && parent_constraint.conenforced {
                        validate_attached_foreign_key_rows_if_needed(
                            self,
                            target,
                            &parent_constraint,
                            &local_attnums,
                            &referenced_attnums,
                            &catalog,
                            client_id,
                            xid,
                            next_cid,
                            std::sync::Arc::clone(&interrupts),
                        )?;
                    }
                    let (constraint_row, effect) = self
                        .catalog
                        .write()
                        .create_foreign_key_constraint_mvcc(
                            target.relation_oid,
                            parent_constraint.conname.clone(),
                            parent_constraint.condeferrable,
                            parent_constraint.condeferred,
                            parent_constraint.conenforced,
                            parent_constraint.convalidated,
                            &local_attnums,
                            parent_constraint.confrelid,
                            parent_constraint.conindid,
                            &referenced_attnums,
                            parent_constraint.confupdtype,
                            parent_constraint.confdeltype,
                            parent_constraint.confmatchtype,
                            delete_set_attnums.as_deref(),
                            parent_constraint.conperiod,
                            parent_constraint.oid,
                            false,
                            1,
                            &ctx,
                        )
                        .map_err(map_catalog_error)?;
                    self.apply_catalog_mutation_effect_immediate(&effect)?;
                    catalog_effects.push(effect);
                    constraint_row
                };
                if parent_constraint.conenforced
                    && catalog
                        .trigger_rows_for_relation(target.relation_oid)
                        .into_iter()
                        .all(|row| row.tgconstraint != constraint_row.oid)
                {
                    next_cid = self.create_foreign_key_check_triggers_in_transaction(
                        client_id,
                        xid,
                        next_cid,
                        &constraint_row,
                        catalog_effects,
                    )?;
                }
            }
        }
        self.reconcile_referenced_partition_foreign_keys_for_attached_child_in_transaction(
            client_id,
            xid,
            next_cid,
            parent_oid,
            child_oid,
            configured_search_path,
            catalog_effects,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn reconcile_referenced_partition_foreign_keys_for_attached_child_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent_oid: u32,
        child_oid: u32,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let latest_catalog = self.lazy_catalog_lookup(
            client_id,
            Some((xid, CommandId::MAX)),
            configured_search_path,
        );
        let parent_constraints = catalog
            .foreign_key_constraint_rows_referencing_relation(parent_oid)
            .into_iter()
            .filter(|row| can_spawn_referenced_partition_foreign_key_clone(row, &catalog))
            .collect::<Vec<_>>();
        if parent_constraints.is_empty() {
            return Ok(cid);
        }
        let parent = catalog
            .relation_by_oid(parent_oid)
            .or_else(|| catalog.lookup_relation_by_oid(parent_oid))
            .or_else(|| latest_catalog.relation_by_oid(parent_oid))
            .or_else(|| latest_catalog.lookup_relation_by_oid(parent_oid))
            .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(parent_oid.to_string())))?;
        let child = catalog
            .relation_by_oid(child_oid)
            .or_else(|| catalog.lookup_relation_by_oid(child_oid))
            .or_else(|| latest_catalog.relation_by_oid(child_oid))
            .or_else(|| latest_catalog.lookup_relation_by_oid(child_oid))
            .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(child_oid.to_string())))?;
        let mut used_names_by_relation =
            std::collections::BTreeMap::<u32, std::collections::BTreeSet<String>>::new();
        let mut next_cid = cid;
        for parent_constraint in parent_constraints {
            if catalog
                .constraint_rows_for_relation(parent_constraint.conrelid)
                .into_iter()
                .any(|row| {
                    row.contype == CONSTRAINT_FOREIGN
                        && row.conparentid == parent_constraint.oid
                        && row.confrelid == child.relation_oid
                })
            {
                continue;
            }
            let parent_referenced_attnums = parent_constraint.confkey.clone().ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "referenced foreign key columns",
                    actual: format!("missing confkey for {}", parent_constraint.conname),
                })
            })?;
            let used_names = used_names_by_relation
                .entry(parent_constraint.conrelid)
                .or_insert_with(|| {
                    catalog
                        .constraint_rows_for_relation(parent_constraint.conrelid)
                        .into_iter()
                        .map(|row| row.conname.to_ascii_lowercase())
                        .collect()
                });
            next_cid = self
                .create_referenced_partition_foreign_key_constraint_for_partition_in_transaction(
                    client_id,
                    xid,
                    next_cid,
                    &parent,
                    &child,
                    &parent_constraint,
                    &parent_referenced_attnums,
                    &parent_constraint.conname,
                    used_names,
                    configured_search_path,
                    catalog_effects,
                )?;
        }
        Ok(next_cid)
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
            if action.enforced && !action.not_valid {
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
                if existing.connoinherit {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "constraint \"{}\" conflicts with inherited constraint on relation \"{}\"",
                            action.constraint_name, child_name
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42P17",
                    });
                }
                if action.enforced && !existing.conenforced {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "constraint \"{}\" conflicts with NOT ENFORCED constraint on relation \"{}\"",
                            action.constraint_name, child_name
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42P17",
                    });
                }
                if action.enforced && !action.not_valid && !existing.convalidated {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "constraint \"{}\" conflicts with NOT VALID constraint on relation \"{}\"",
                            action.constraint_name, child_name
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42P17",
                    });
                }
                push_notice(format!(
                    "merging constraint \"{}\" with inherited definition",
                    action.constraint_name
                ));
                let conenforced = existing.conenforced || action.enforced;
                let convalidated = conenforced && (existing.convalidated || action.enforced);
                let effect = self
                    .catalog
                    .write()
                    .update_check_constraint_state_mvcc(
                        child_relation.relation_oid,
                        existing.oid,
                        Some(conenforced),
                        Some(convalidated),
                        Some(parent_constraint_oid),
                        Some(existing.conislocal),
                        Some(existing.coninhcount.saturating_add(1)),
                        Some(false),
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

    #[allow(clippy::too_many_arguments)]
    fn propagate_not_null_constraint_to_inheritors(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent_relation: &crate::backend::parser::BoundRelation,
        action: &crate::backend::parser::NotNullConstraintAction,
        no_inherit_conflict_hint: bool,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let mut visited = std::collections::BTreeSet::new();
        self.propagate_not_null_constraint_to_inheritors_inner(
            client_id,
            xid,
            cid,
            parent_relation,
            action,
            no_inherit_conflict_hint,
            configured_search_path,
            catalog_effects,
            &mut visited,
        )?;
        self.recompute_not_null_inheritance_counts_for_descendants(
            client_id,
            xid,
            cid.saturating_add(1),
            parent_relation.relation_oid,
            &action.column,
            configured_search_path,
            catalog_effects,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn recompute_not_null_inheritance_counts_for_descendants(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent_oid: u32,
        column_name: &str,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let mut descendants = Vec::new();
        let lookup_cid = cid.saturating_add(catalog_effects.len() as u32);
        let catalog =
            self.lazy_catalog_lookup(client_id, Some((xid, lookup_cid)), configured_search_path);
        collect_inheritance_descendant_oids(&catalog, parent_oid, &mut descendants);
        let shape_catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let mut seen = std::collections::BTreeSet::new();
        for relation_oid in descendants {
            if !seen.insert(relation_oid) {
                continue;
            }
            let lookup_cid = cid
                .saturating_add(1)
                .saturating_add(catalog_effects.len() as u32);
            let catalog = self.lazy_catalog_lookup(
                client_id,
                Some((xid, lookup_cid)),
                configured_search_path,
            );
            let Some(row) = not_null_constraint_for_column_with_shape(
                &catalog,
                &shape_catalog,
                relation_oid,
                column_name,
            )?
            else {
                continue;
            };
            let Some(attnum) = row.conkey.as_ref().and_then(|keys| keys.first()).copied() else {
                continue;
            };
            let inhcount = inherited_not_null_parent_count_with_shape(
                &catalog,
                &shape_catalog,
                relation_oid,
                column_name,
            )?;
            if row.coninhcount == inhcount && !row.connoinherit {
                continue;
            }
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: cid
                    .saturating_add(1)
                    .saturating_add(catalog_effects.len() as u32),
                client_id,
                waiter: None,
                interrupts: self.interrupt_state(client_id),
            };
            let effect = self
                .catalog
                .write()
                .alter_not_null_constraint_state_by_attnum_mvcc(
                    relation_oid,
                    attnum,
                    row.oid,
                    &row.conname,
                    None,
                    Some(false),
                    Some(row.conislocal),
                    Some(inhcount),
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn propagate_not_null_constraint_to_inheritors_inner(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent_relation: &crate::backend::parser::BoundRelation,
        action: &crate::backend::parser::NotNullConstraintAction,
        no_inherit_conflict_hint: bool,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        visited: &mut std::collections::BTreeSet<u32>,
    ) -> Result<(), ExecError> {
        let children = {
            let catalog =
                self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
            let shape_catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
            direct_inheritance_children(&catalog, parent_relation.relation_oid)?
                .into_iter()
                .map(|relation| relation_with_shape(&shape_catalog, relation))
                .collect::<Vec<_>>()
        };
        for child_relation in children {
            if !visited.insert(child_relation.relation_oid) {
                continue;
            }
            let catalog =
                self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
            let shape_catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
            let child_name = relation_basename(
                &catalog
                    .class_row_by_oid(child_relation.relation_oid)
                    .map(|row| row.relname)
                    .unwrap_or_else(|| child_relation.relation_oid.to_string()),
            )
            .to_string();
            let column_index = match relation_column_index_by_name(&child_relation, &action.column)
            {
                Ok(index) => index,
                Err(ExecError::Parse(ParseError::UnknownColumn(_))) => continue,
                Err(err) => return Err(err),
            };
            let column_name = child_relation.desc.columns[column_index].name.clone();
            if !action.not_valid {
                validate_not_null_rows(
                    self,
                    &child_relation,
                    &child_name,
                    column_index,
                    &action.constraint_name,
                    &catalog,
                    client_id,
                    xid,
                    cid,
                    self.interrupt_state(client_id),
                )?;
            }

            let existing = match not_null_constraint_for_column_with_shape(
                &catalog,
                &shape_catalog,
                child_relation.relation_oid,
                &column_name,
            ) {
                Ok(existing) => existing,
                Err(ExecError::Parse(ParseError::UnknownColumn(_))) => continue,
                Err(err) => return Err(err),
            };
            let child_inhcount = inherited_not_null_parent_count_with_shape(
                &catalog,
                &shape_catalog,
                child_relation.relation_oid,
                &column_name,
            )?
            .max(1);
            let constraint_ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: cid.saturating_add(catalog_effects.len() as u32),
                client_id,
                waiter: None,
                interrupts: self.interrupt_state(client_id),
            };
            let child_constraint_name = if let Some(existing) = existing {
                if existing.connoinherit {
                    return Err(cannot_change_not_null_no_inherit_error(
                        &existing.conname,
                        &child_name,
                        no_inherit_conflict_hint,
                    ));
                }
                let effect = self
                    .catalog
                    .write()
                    .alter_not_null_constraint_state_by_attnum_mvcc(
                        child_relation.relation_oid,
                        *attnums_from_constraint(&existing)?
                            .first()
                            .expect("not null attnum"),
                        existing.oid,
                        &existing.conname,
                        (!action.not_valid).then_some(true),
                        Some(false),
                        Some(existing.conislocal),
                        Some(child_inhcount),
                        &constraint_ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                existing.conname
            } else {
                let (constraint_oid, effect) = self
                    .catalog
                    .write()
                    .set_column_not_null_mvcc(
                        child_relation.relation_oid,
                        &column_name,
                        action.constraint_name.clone(),
                        !action.not_valid,
                        false,
                        false,
                        &constraint_ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);

                let inherit_ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: cid.saturating_add(catalog_effects.len() as u32),
                    client_id,
                    waiter: None,
                    interrupts: self.interrupt_state(client_id),
                };
                let effect = self
                    .catalog
                    .write()
                    .alter_not_null_constraint_state_by_attnum_mvcc(
                        child_relation.relation_oid,
                        (column_index + 1) as i16,
                        constraint_oid,
                        &action.constraint_name,
                        None,
                        Some(false),
                        Some(false),
                        Some(child_inhcount),
                        &inherit_ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                let _ = constraint_oid;
                action.constraint_name.clone()
            };

            let child_action = crate::backend::parser::NotNullConstraintAction {
                constraint_name: child_constraint_name,
                column: column_name,
                not_valid: action.not_valid,
                no_inherit: false,
                primary_key_owned: false,
                is_local: false,
                inhcount: child_inhcount,
            };
            self.propagate_not_null_constraint_to_inheritors_inner(
                client_id,
                xid,
                cid.saturating_add(1),
                &child_relation,
                &child_action,
                no_inherit_conflict_hint,
                configured_search_path,
                catalog_effects,
                visited,
            )?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn drop_check_constraint_from_inheritors(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent_relation: &crate::backend::parser::BoundRelation,
        parent_constraint: &PgConstraintRow,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let child_relations = direct_inheritance_children(&catalog, parent_relation.relation_oid)?;
        for child_relation in child_relations {
            let lookup_cid = cid
                .saturating_add(catalog_effects.len() as u32)
                .saturating_add(1);
            let catalog = self.lazy_catalog_lookup(
                client_id,
                Some((xid, lookup_cid)),
                configured_search_path,
            );
            let child_relation = catalog
                .lookup_relation_by_oid(child_relation.relation_oid)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnknownTable(
                        child_relation.relation_oid.to_string(),
                    ))
                })?;
            let child_constraints = catalog
                .constraint_rows()
                .into_iter()
                .filter(|row| row.conrelid == child_relation.relation_oid)
                .collect::<Vec<_>>();
            let Some(child_constraint) = child_constraints
                .iter()
                .find(|row| {
                    row.contype == CONSTRAINT_CHECK
                        && row.conname.eq_ignore_ascii_case(&parent_constraint.conname)
                        && row.coninhcount > 0
                })
                .cloned()
            else {
                continue;
            };

            let next_inhcount = child_constraint.coninhcount.saturating_sub(1);
            let remove_child_constraint = next_inhcount == 0 && !child_constraint.conislocal;
            let constraint_ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: cid.saturating_add(catalog_effects.len() as u32),
                client_id,
                waiter: None,
                interrupts: self.interrupt_state(client_id),
            };
            let effect = if remove_child_constraint {
                let (_removed, effect) = self
                    .catalog
                    .write()
                    .drop_relation_constraint_mvcc(
                        child_relation.relation_oid,
                        &child_constraint.conname,
                        &constraint_ctx,
                    )
                    .map_err(map_catalog_error)?;
                effect
            } else {
                self.catalog
                    .write()
                    .update_check_constraint_state_mvcc(
                        child_relation.relation_oid,
                        child_constraint.oid,
                        None,
                        None,
                        Some(if next_inhcount == 0 {
                            0
                        } else {
                            child_constraint.conparentid
                        }),
                        Some(child_constraint.conislocal || next_inhcount == 0),
                        Some(next_inhcount),
                        Some(if child_constraint.conislocal {
                            child_constraint.connoinherit
                        } else {
                            false
                        }),
                        &constraint_ctx,
                    )
                    .map_err(map_catalog_error)?
            };
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);

            if remove_child_constraint {
                self.drop_check_constraint_from_inheritors(
                    client_id,
                    xid,
                    cid.saturating_add(1),
                    &child_relation,
                    &child_constraint,
                    configured_search_path,
                    catalog_effects,
                )?;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn drop_not_null_constraint_from_inheritors(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent_relation: &crate::backend::parser::BoundRelation,
        parent_constraint: &PgConstraintRow,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let parent_attnum = *attnums_from_constraint(parent_constraint)?
            .first()
            .expect("not null attnum");
        let parent_column_index = column_index_for_attnum(parent_relation, parent_attnum)?;
        let parent_column_name = parent_relation.desc.columns[parent_column_index]
            .name
            .clone();
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let child_relations = direct_inheritance_children(&catalog, parent_relation.relation_oid)?;
        for child_relation in child_relations {
            let lookup_cid = cid
                .saturating_add(catalog_effects.len() as u32)
                .saturating_add(1);
            let catalog = self.lazy_catalog_lookup(
                client_id,
                Some((xid, lookup_cid)),
                configured_search_path,
            );
            let child_relation = catalog
                .lookup_relation_by_oid(child_relation.relation_oid)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnknownTable(
                        child_relation.relation_oid.to_string(),
                    ))
                })?;
            let child_display_name = catalog
                .class_row_by_oid(child_relation.relation_oid)
                .map(|row| row.relname)
                .unwrap_or_else(|| child_relation.relation_oid.to_string());
            ensure_relation_owner(self, client_id, &child_relation, &child_display_name)?;
            let child_column_index =
                relation_column_index_by_name(&child_relation, &parent_column_name)?;
            let child_column_name = child_relation.desc.columns[child_column_index].name.clone();
            let Some(child_constraint) =
                not_null_constraint_for_column(&catalog, &child_relation, &child_column_name)?
            else {
                continue;
            };
            if child_constraint.coninhcount == 0 {
                continue;
            }

            let next_inhcount = child_constraint.coninhcount.saturating_sub(1);
            let remove_child_constraint = next_inhcount == 0 && !child_constraint.conislocal;
            let constraint_ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: cid.saturating_add(catalog_effects.len() as u32),
                client_id,
                waiter: None,
                interrupts: self.interrupt_state(client_id),
            };
            let effect = if remove_child_constraint {
                self.catalog
                    .write()
                    .drop_column_not_null_mvcc(
                        child_relation.relation_oid,
                        &child_column_name,
                        &constraint_ctx,
                    )
                    .map_err(map_catalog_error)?
            } else {
                self.catalog
                    .write()
                    .alter_not_null_constraint_state_by_attnum_mvcc(
                        child_relation.relation_oid,
                        (child_column_index + 1) as i16,
                        child_constraint.oid,
                        &child_constraint.conname,
                        None,
                        Some(if child_constraint.conislocal {
                            child_constraint.connoinherit
                        } else {
                            false
                        }),
                        Some(child_constraint.conislocal || next_inhcount == 0),
                        Some(next_inhcount),
                        &constraint_ctx,
                    )
                    .map_err(map_catalog_error)?
            };
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);

            if remove_child_constraint {
                self.drop_not_null_constraint_from_inheritors(
                    client_id,
                    xid,
                    cid.saturating_add(1),
                    &child_relation,
                    &child_constraint,
                    configured_search_path,
                    catalog_effects,
                )?;
            }
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
        let Some(relation) = lookup_table_or_partitioned_table_for_alter_table(
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
        let Some(relation) = lookup_table_or_partitioned_table_for_alter_table(
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
                        "constraint \"{}\" of relation \"{}\" does not exist",
                        drop_stmt.constraint_name,
                        relation_basename(&drop_stmt.table_name)
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
                if row.contype == CONSTRAINT_CHECK && row.coninhcount > 0 {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "cannot drop inherited constraint \"{}\" of relation \"{}\"",
                            row.conname,
                            relation_basename(&drop_stmt.table_name),
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42P16",
                    });
                }
                if row.contype == CONSTRAINT_FOREIGN {
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
                    self.drop_partition_child_foreign_key_constraints_in_transaction(
                        client_id,
                        xid,
                        cid,
                        &row,
                        &catalog,
                        catalog_effects,
                    )?;
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
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                if row.contype == CONSTRAINT_CHECK && !row.connoinherit {
                    self.drop_check_constraint_from_inheritors(
                        client_id,
                        xid,
                        cid.saturating_add(1),
                        &relation,
                        &row,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
            }
            CONSTRAINT_NOTNULL => {
                if row.coninhcount > 0 {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "cannot drop inherited constraint \"{}\" of relation \"{}\"",
                            row.conname,
                            relation_basename(&drop_stmt.table_name),
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42P16",
                    });
                }
                let attnum = *attnums_from_constraint(&row)?
                    .first()
                    .expect("not null attnum");
                let column_index = column_index_for_attnum(&relation, attnum)?;
                if primary_constraint_for_attnum(&rows, attnum).is_some() {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "droppable NOT NULL constraint",
                        actual: format!(
                            "column \"{}\" is in a primary key",
                            relation.desc.columns[column_index].name
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
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                if !row.connoinherit {
                    self.drop_not_null_constraint_from_inheritors(
                        client_id,
                        xid,
                        cid.saturating_add(1),
                        &relation,
                        &row,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
            }
            CONSTRAINT_PRIMARY | CONSTRAINT_UNIQUE | CONSTRAINT_EXCLUSION => {
                if row.conparentid != 0 || row.coninhcount > 0 {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "cannot drop inherited constraint \"{}\" of relation \"{}\"",
                            row.conname,
                            relation_basename(&drop_stmt.table_name),
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42P16",
                    });
                }
                reject_constraint_with_dependent_rule(
                    self,
                    client_id,
                    Some((xid, cid)),
                    row.oid,
                    &row.conname,
                    relation_basename(&drop_stmt.table_name),
                )?;
                if row.conindid != 0 {
                    reject_index_with_referencing_foreign_keys(
                        &catalog,
                        row.conindid,
                        "ALTER TABLE DROP CONSTRAINT on unreferenced key",
                    )?;
                }
                let mut next_cid = self
                    .drop_partition_child_index_backed_constraints_in_transaction(
                        client_id,
                        xid,
                        cid,
                        &row,
                        &catalog,
                        catalog_effects,
                    )?;
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
        let Some(relation) = lookup_table_partitioned_or_view_for_alter_table(
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
        self.table_locks.unlock_table(lock_tag, client_id);
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
        let Some(relation) = lookup_table_partitioned_or_view_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        if relation.relkind == 'v' {
            return Err(unsupported_view_alter_action(
                "ALTER COLUMN ... SET NOT NULL",
                &alter_stmt.table_name,
            ));
        }
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
        let children = direct_inheritance_children(&catalog, relation.relation_oid)?;
        let has_children = !children.is_empty();
        let existing_constraints = catalog.constraint_rows_for_relation(relation.relation_oid);
        let existing_not_null =
            not_null_constraint_for_attnum(&existing_constraints, (column_index + 1) as i16)
                .cloned();
        if !relation.desc.columns[column_index].storage.nullable {
            let constraint_name = relation.desc.columns[column_index]
                .not_null_constraint_name
                .as_deref()
                .unwrap_or(&alter_stmt.column_name);
            if relation.desc.columns[column_index].not_null_constraint_no_inherit
                && !alter_stmt.only
            {
                return Err(cannot_change_not_null_no_inherit_error(
                    constraint_name,
                    relation_basename(&alter_stmt.table_name),
                    false,
                ));
            }
            if let Some(row) = existing_not_null
                && !row.convalidated
            {
                let validate_stmt = crate::backend::parser::AlterTableValidateConstraintStatement {
                    if_exists: false,
                    only: alter_stmt.only,
                    table_name: alter_stmt.table_name.clone(),
                    constraint_name: row.conname,
                };
                return self
                    .execute_alter_table_validate_constraint_stmt_in_transaction_with_search_path(
                        client_id,
                        &validate_stmt,
                        xid,
                        cid,
                        configured_search_path,
                        catalog_effects,
                    );
            }
            return Ok(StatementResult::AffectedRows(0));
        }
        let no_inherit = if alter_stmt.only && has_children {
            if relation.relkind == 'p' {
                return Err(ExecError::DetailedError {
                    message: "constraint must be added to child tables too".into(),
                    detail: None,
                    hint: Some("Do not specify the ONLY keyword.".into()),
                    sqlstate: "42P16",
                });
            }
            true
        } else {
            false
        };
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
                constraint_name.clone(),
                true,
                no_inherit,
                false,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        let (_constraint_oid, effect) = effect;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        if !alter_stmt.only && !no_inherit {
            let action = crate::backend::parser::NotNullConstraintAction {
                constraint_name,
                column: relation.desc.columns[column_index].name.clone(),
                not_valid: false,
                no_inherit: false,
                primary_key_owned: false,
                is_local: true,
                inhcount: 0,
            };
            self.propagate_not_null_constraint_to_inheritors(
                client_id,
                xid,
                cid.saturating_add(1),
                &relation,
                &action,
                true,
                configured_search_path,
                catalog_effects,
            )?;
        }
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
        let Some(relation) = lookup_table_partitioned_or_view_for_alter_table(
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
        self.table_locks.unlock_table(lock_tag, client_id);
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
        let Some(relation) = lookup_table_partitioned_or_view_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        if relation.relkind == 'v' {
            return Err(unsupported_view_alter_action(
                "ALTER COLUMN ... DROP NOT NULL",
                &alter_stmt.table_name,
            ));
        }
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
                    "column \"{}\" is in a primary key",
                    relation.desc.columns[column_index].name
                ),
            }));
        }
        let existing_not_null = not_null_constraint_for_attnum(&existing_constraints, attnum)
            .cloned()
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "existing not-null constraint",
                    actual: format!(
                        "missing not-null constraint for column \"{}\"",
                        relation.desc.columns[column_index].name
                    ),
                })
            })?;
        if existing_not_null.coninhcount > 0 {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot drop inherited constraint \"{}\" of relation \"{}\"",
                    existing_not_null.conname,
                    relation_basename(&alter_stmt.table_name),
                ),
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
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        self.drop_not_null_constraint_from_inheritors(
            client_id,
            xid,
            cid.saturating_add(1),
            &relation,
            &existing_not_null,
            configured_search_path,
            catalog_effects,
        )?;
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
        let Some(relation) = lookup_table_or_partitioned_table_for_alter_table(
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
                let column_name = relation.desc.columns[column_index].name.clone();
                if !row.connoinherit {
                    let inheritors = catalog.find_all_inheritors(relation.relation_oid);
                    if alter_stmt.only && inheritors.iter().any(|oid| *oid != relation.relation_oid)
                    {
                        return Err(ExecError::DetailedError {
                            message: "constraint must be validated on child tables too".into(),
                            detail: None,
                            hint: None,
                            sqlstate: "42P16",
                        });
                    }
                    for child_oid in inheritors {
                        if child_oid == relation.relation_oid {
                            continue;
                        }
                        let child = catalog.lookup_relation_by_oid(child_oid).ok_or_else(|| {
                            ExecError::Parse(ParseError::UnknownTable(child_oid.to_string()))
                        })?;
                        let Some(child_row) =
                            not_null_constraint_for_column(&catalog, &child, &column_name)?
                        else {
                            continue;
                        };
                        if child_row.convalidated {
                            continue;
                        }
                        let child_column_index =
                            relation_column_index_by_name(&child, &column_name)?;
                        let child_name = catalog
                            .class_row_by_oid(child.relation_oid)
                            .map(|row| row.relname)
                            .unwrap_or_else(|| child.relation_oid.to_string());
                        validate_not_null_rows(
                            self,
                            &child,
                            &child_name,
                            child_column_index,
                            &child_row.conname,
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
                            interrupts: std::sync::Arc::clone(&interrupts),
                        };
                        let effect = self
                            .catalog
                            .write()
                            .validate_not_null_constraint_mvcc(
                                child.relation_oid,
                                &child_row.conname,
                                &ctx,
                            )
                            .map_err(map_catalog_error)?;
                        catalog_effects.push(effect);
                    }
                }
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
                if !row.connoinherit {
                    let inheritors = catalog.find_all_inheritors(relation.relation_oid);
                    if alter_stmt.only && inheritors.iter().any(|oid| *oid != relation.relation_oid)
                    {
                        return Err(ExecError::DetailedError {
                            message: "constraint must be validated on child tables too".into(),
                            detail: None,
                            hint: None,
                            sqlstate: "42P16",
                        });
                    }
                    for child_oid in inheritors {
                        if child_oid == relation.relation_oid {
                            continue;
                        }
                        let child = catalog.lookup_relation_by_oid(child_oid).ok_or_else(|| {
                            ExecError::Parse(ParseError::UnknownTable(child_oid.to_string()))
                        })?;
                        let child_row = catalog
                            .constraint_rows_for_relation(child.relation_oid)
                            .into_iter()
                            .find(|child_row| {
                                child_row.contype == CONSTRAINT_CHECK
                                    && child_row.conname.eq_ignore_ascii_case(&row.conname)
                            })
                            .ok_or_else(|| {
                                ExecError::Parse(ParseError::UnexpectedToken {
                                    expected: "inherited CHECK constraint",
                                    actual: format!(
                                        "constraint \"{}\" of relation \"{}\" does not exist",
                                        row.conname, child_oid
                                    ),
                                })
                            })?;
                        if child_row.convalidated {
                            continue;
                        }
                        let child_expr_sql = child_row.conbin.clone().ok_or_else(|| {
                            ExecError::Parse(ParseError::UnexpectedToken {
                                expected: "stored CHECK constraint expression",
                                actual: format!(
                                    "missing expression for constraint {}",
                                    child_row.conname
                                ),
                            })
                        })?;
                        let child_name = catalog
                            .class_row_by_oid(child.relation_oid)
                            .map(|row| row.relname)
                            .unwrap_or_else(|| child.relation_oid.to_string());
                        validate_check_rows(
                            self,
                            &child,
                            &child_name,
                            &child_row.conname,
                            &child_expr_sql,
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
                            interrupts: std::sync::Arc::clone(&interrupts),
                        };
                        let effect = self
                            .catalog
                            .write()
                            .validate_check_constraint_mvcc(
                                child.relation_oid,
                                &child_row.conname,
                                &ctx,
                            )
                            .map_err(map_catalog_error)?;
                        catalog_effects.push(effect);
                    }
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
                let references_partitioned_table = catalog
                    .relation_by_oid(row.confrelid)
                    .is_some_and(|relation| relation.relkind == 'p');
                if relation.relkind != 'p' && !references_partitioned_table {
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
                self.validate_partition_child_foreign_key_constraints_in_transaction(
                    client_id,
                    xid,
                    cid.saturating_add(catalog_effects.len() as u32),
                    &row,
                    &catalog,
                    catalog_effects,
                )?;
            }
            _ => {}
        }

        Ok(StatementResult::AffectedRows(0))
    }

    fn validate_partition_child_foreign_key_constraints_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        parent_constraint: &PgConstraintRow,
        catalog: &dyn CatalogLookup,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let all_rows = catalog.constraint_rows();
        let mut pending = all_rows
            .iter()
            .filter(|row| {
                row.contype == CONSTRAINT_FOREIGN && row.conparentid == parent_constraint.oid
            })
            .cloned()
            .map(|row| (parent_constraint.clone(), row))
            .collect::<Vec<_>>();
        let interrupts = self.interrupt_state(client_id);
        let datetime_config = crate::backend::utils::misc::guc_datetime::DateTimeConfig::default();
        let mut next_cid = cid;
        while let Some((parent_row, child_row)) = pending.pop() {
            pending.extend(
                all_rows
                    .iter()
                    .filter(|row| {
                        row.contype == CONSTRAINT_FOREIGN && row.conparentid == child_row.oid
                    })
                    .cloned()
                    .map(|row| (child_row.clone(), row)),
            );
            let child_relation = catalog.relation_by_oid(child_row.conrelid).ok_or_else(|| {
                ExecError::Parse(ParseError::UnknownTable(child_row.conrelid.to_string()))
            })?;
            if !child_row.conenforced || child_row.convalidated {
                continue;
            }
            let referenced_side = parent_row.conrelid == child_row.conrelid;
            let references_partitioned_table = catalog
                .relation_by_oid(child_row.confrelid)
                .is_some_and(|relation| relation.relkind == 'p');
            let relation_name = relation_basename(
                &catalog
                    .class_row_by_oid(child_relation.relation_oid)
                    .map(|row| row.relname)
                    .unwrap_or_else(|| child_relation.relation_oid.to_string()),
            )
            .to_string();
            if !referenced_side && child_relation.relkind != 'p' && !references_partitioned_table {
                let constraints = crate::backend::parser::bind_relation_constraints(
                    Some(&relation_name),
                    child_relation.relation_oid,
                    &child_relation.desc,
                    catalog,
                )
                .map_err(ExecError::Parse)?;
                let constraint = constraints
                    .foreign_keys
                    .into_iter()
                    .find(|constraint| constraint.constraint_oid == child_row.oid)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "bound foreign key constraint",
                            actual: format!(
                                "missing foreign key binding for {}",
                                child_row.conname
                            ),
                        })
                    })?;
                let mut ctx = ddl_executor_context(
                    self,
                    catalog,
                    client_id,
                    xid,
                    next_cid,
                    &datetime_config,
                    std::sync::Arc::clone(&interrupts),
                )?;
                let rows = collect_matching_rows_heap(
                    child_relation.rel,
                    &child_relation.desc,
                    child_relation.toast,
                    None,
                    &mut ctx,
                )?;
                for (_, values) in rows {
                    crate::backend::executor::enforce_outbound_foreign_keys(
                        &relation_name,
                        std::slice::from_ref(&constraint),
                        None,
                        &values,
                        &mut ctx,
                    )?;
                }
            }
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
                .validate_foreign_key_constraint_mvcc(
                    child_relation.relation_oid,
                    &child_row.conname,
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            next_cid = next_cid.saturating_add(1);
        }
        Ok(next_cid)
    }

    pub(super) fn validate_referenced_partition_foreign_keys_for_detach_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        detached_oid: u32,
        configured_search_path: Option<&[String]>,
    ) -> Result<(), ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let detached = catalog
            .relation_by_oid(detached_oid)
            .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(detached_oid.to_string())))?;
        let mut subtree = vec![detached.clone()];
        subtree.extend(partition_descendants(&catalog, detached_oid)?);
        let subtree_oids = subtree
            .iter()
            .map(|relation| relation.relation_oid)
            .collect::<std::collections::BTreeSet<_>>();
        let mut clone_rows = catalog
            .constraint_rows()
            .into_iter()
            .filter(|row| {
                row.contype == CONSTRAINT_FOREIGN
                    && subtree_oids.contains(&row.confrelid)
                    && is_referenced_side_foreign_key_clone(row, &catalog)
            })
            .collect::<Vec<_>>();
        clone_rows.sort_by_key(|row| {
            let position = subtree
                .iter()
                .position(|relation| relation.relation_oid == row.confrelid)
                .unwrap_or(usize::MAX);
            (position, row.oid)
        });
        let datetime_config = crate::backend::utils::misc::guc_datetime::DateTimeConfig::default();
        let interrupts = self.interrupt_state(client_id);
        for row in clone_rows {
            if !row.conenforced {
                continue;
            }
            let referenced_relation = catalog.relation_by_oid(row.confrelid).ok_or_else(|| {
                ExecError::Parse(ParseError::UnknownTable(row.confrelid.to_string()))
            })?;
            let constraints = crate::backend::parser::bind_referenced_by_foreign_keys(
                referenced_relation.relation_oid,
                &referenced_relation.desc,
                &catalog,
            )
            .map_err(ExecError::Parse)?;
            let Some(constraint) = constraints
                .into_iter()
                .find(|constraint| constraint.constraint_oid == row.oid)
            else {
                continue;
            };
            let scan_relations = if referenced_relation.relkind == 'p' {
                partition_descendants(&catalog, referenced_relation.relation_oid)?
                    .into_iter()
                    .filter(|relation| relation.relkind != 'p')
                    .collect::<Vec<_>>()
            } else {
                vec![referenced_relation.clone()]
            };
            for scan_relation in scan_relations {
                let referenced_attnums = row.confkey.clone().ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "referenced foreign key columns",
                        actual: format!("missing confkey for {}", row.conname),
                    })
                })?;
                let scan_attnums = attnums_by_parent_column_names(
                    &referenced_relation.desc,
                    &scan_relation.desc,
                    &referenced_attnums,
                )?;
                let scan_indexes = scan_attnums
                    .iter()
                    .map(|attnum| {
                        usize::try_from(attnum.saturating_sub(1)).map_err(|_| {
                            ExecError::Parse(ParseError::UnexpectedToken {
                                expected: "referenced foreign key attnum",
                                actual: attnum.to_string(),
                            })
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mut ctx = ddl_executor_context(
                    self,
                    &catalog,
                    client_id,
                    xid,
                    cid,
                    &datetime_config,
                    std::sync::Arc::clone(&interrupts),
                )?;
                let rows = collect_matching_rows_heap(
                    scan_relation.rel,
                    &scan_relation.desc,
                    scan_relation.toast,
                    None,
                    &mut ctx,
                )?;
                for (_, values) in rows {
                    let key_values = scan_indexes
                        .iter()
                        .map(|index| values[*index].to_owned_value())
                        .collect::<Vec<_>>();
                    match crate::backend::executor::enforce_inbound_foreign_key_reference(
                        relation_basename(
                            &catalog
                                .class_row_by_oid(referenced_relation.relation_oid)
                                .map(|row| row.relname)
                                .unwrap_or_else(|| referenced_relation.relation_oid.to_string()),
                        ),
                        &constraint,
                        &key_values,
                        &mut ctx,
                    ) {
                        Ok(()) => {}
                        Err(ExecError::ForeignKeyViolation { .. }) => {
                            let detached_name = catalog
                                .class_row_by_oid(detached.relation_oid)
                                .map(|row| row.relname)
                                .unwrap_or_else(|| detached.relation_oid.to_string());
                            return Err(ExecError::ForeignKeyViolation {
                                constraint: constraint.display_constraint_name.clone(),
                                message: format!(
                                    "removing partition \"{}\" violates foreign key constraint \"{}\"",
                                    relation_basename(&detached_name),
                                    constraint.display_constraint_name
                                ),
                                detail: Some(format!(
                                    "Key ({})=({}) is still referenced from table \"{}\".",
                                    constraint.referenced_column_names.join(", "),
                                    render_detach_foreign_key_values(&key_values),
                                    constraint.display_child_relation_name
                                )),
                            });
                        }
                        Err(err) => return Err(err),
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn drop_referenced_partition_foreign_key_constraints_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        detached_oid: u32,
        catalog: &dyn CatalogLookup,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let mut subtree =
            vec![catalog.relation_by_oid(detached_oid).ok_or_else(|| {
                ExecError::Parse(ParseError::UnknownTable(detached_oid.to_string()))
            })?];
        subtree.extend(partition_descendants(catalog, detached_oid)?);
        let subtree_oids = subtree
            .iter()
            .map(|relation| relation.relation_oid)
            .collect::<std::collections::BTreeSet<_>>();
        let mut rows = catalog
            .constraint_rows()
            .into_iter()
            .filter(|row| {
                row.contype == CONSTRAINT_FOREIGN
                    && subtree_oids.contains(&row.confrelid)
                    && is_referenced_side_foreign_key_clone(row, catalog)
            })
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| std::cmp::Reverse(row.oid));
        let interrupts = self.interrupt_state(client_id);
        let mut next_cid = cid;
        for row in rows {
            self.drop_foreign_key_triggers_in_transaction(
                client_id,
                xid,
                next_cid,
                &row,
                catalog,
                catalog_effects,
            )?;
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: next_cid.saturating_add(catalog_effects.len() as u32),
                client_id,
                waiter: None,
                interrupts: std::sync::Arc::clone(&interrupts),
            };
            let (_removed, effect) = self
                .catalog
                .write()
                .drop_relation_constraint_mvcc(row.conrelid, &row.conname, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            next_cid = next_cid.saturating_add(1);
        }
        Ok(next_cid)
    }
}

fn render_detach_foreign_key_values(values: &[Value]) -> String {
    values
        .iter()
        .map(|value| match value {
            Value::Null => "null".into(),
            Value::Int16(v) => v.to_string(),
            Value::Int32(v) => v.to_string(),
            Value::Int64(v) => v.to_string(),
            Value::Bool(v) => v.to_string(),
            Value::Text(text) => text.to_string(),
            Value::TextRef(_, _) => value.as_text().unwrap_or_default().to_string(),
            _ => format!("{value:?}"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}
