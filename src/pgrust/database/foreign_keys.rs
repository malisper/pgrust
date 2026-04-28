use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use crate::ClientId;
use crate::backend::access::index::unique::probe_unique_conflict;
use crate::backend::access::transam::xact::{CommandId, TransactionId};
use crate::backend::commands::tablecmds::{
    build_immediate_index_insert_context, collect_matching_rows_heap,
};
use crate::backend::executor::{
    ConstraintTiming, DeferredForeignKeyTracker, ExecError, ExecutorContext,
    PendingParentForeignKeyCheck, StatementResult, enforce_deferred_inbound_foreign_key_check,
    enforce_outbound_foreign_keys,
};
use crate::backend::parser::{
    AlterTableAddConstraintStatement, AlterTableValidateConstraintStatement, BoundDeleteStatement,
    BoundInsertStatement, BoundRelation, BoundRelationConstraints, BoundUpdateStatement,
    CatalogLookup, ParseError, PreparedInsert, QualifiedNameRef, SetConstraintsStatement,
    bind_referenced_by_foreign_keys, bind_relation_constraints,
    normalize_alter_table_add_constraint,
};
use crate::backend::storage::lmgr::TableLockMode;
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::utils::cache::lsyscache::LazyCatalogLookup;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::misc::interrupts::InterruptState;
use crate::include::catalog::{CONSTRAINT_FOREIGN, CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE};

use super::Database;

pub(crate) type TableLockRequest = (RelFileLocator, TableLockMode);

pub(crate) fn insert_foreign_key_lock_requests(
    stmt: &BoundInsertStatement,
) -> Vec<TableLockRequest> {
    let mut requests = BTreeMap::new();
    add_lock_request(&mut requests, stmt.rel, TableLockMode::RowExclusive);
    add_relation_foreign_key_partner_locks(&mut requests, &stmt.relation_constraints);
    requests.into_iter().collect()
}

pub(crate) fn prepared_insert_foreign_key_lock_requests(
    prepared: &PreparedInsert,
) -> Vec<TableLockRequest> {
    let mut requests = BTreeMap::new();
    add_lock_request(&mut requests, prepared.rel, TableLockMode::RowExclusive);
    add_relation_foreign_key_partner_locks(&mut requests, &prepared.relation_constraints);
    requests.into_iter().collect()
}

pub(crate) fn update_foreign_key_lock_requests(
    stmt: &BoundUpdateStatement,
) -> Vec<TableLockRequest> {
    let mut requests = BTreeMap::new();
    for target in &stmt.targets {
        add_lock_request(&mut requests, target.rel, TableLockMode::RowExclusive);
        add_relation_foreign_key_partner_locks(&mut requests, &target.relation_constraints);
        for constraint in &target.referenced_by_foreign_keys {
            add_lock_request(
                &mut requests,
                constraint.child_rel,
                TableLockMode::ShareUpdateExclusive,
            );
        }
    }
    requests.into_iter().collect()
}

pub(crate) fn delete_foreign_key_lock_requests(
    stmt: &BoundDeleteStatement,
) -> Vec<TableLockRequest> {
    let mut requests = BTreeMap::new();
    for target in &stmt.targets {
        add_lock_request(&mut requests, target.rel, TableLockMode::RowExclusive);
        for constraint in &target.referenced_by_foreign_keys {
            add_lock_request(
                &mut requests,
                constraint.child_rel,
                TableLockMode::ShareUpdateExclusive,
            );
        }
    }
    requests.into_iter().collect()
}

pub(crate) fn relation_foreign_key_lock_requests(
    rel: RelFileLocator,
    constraints: &BoundRelationConstraints,
) -> Vec<TableLockRequest> {
    let mut requests = BTreeMap::new();
    add_lock_request(&mut requests, rel, TableLockMode::RowExclusive);
    add_relation_foreign_key_partner_locks(&mut requests, constraints);
    requests.into_iter().collect()
}

pub(crate) fn alter_table_add_constraint_lock_requests(
    relation: &BoundRelation,
    stmt: &AlterTableAddConstraintStatement,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<TableLockRequest>, ExecError> {
    let mut requests = BTreeMap::new();
    add_lock_request(&mut requests, relation.rel, TableLockMode::AccessExclusive);

    let normalized = normalize_alter_table_add_constraint(
        relation_basename(&stmt.table_name),
        relation.relation_oid,
        relation.relpersistence,
        &relation.desc,
        &catalog.constraint_rows_for_relation(relation.relation_oid),
        &stmt.constraint,
        catalog,
    )
    .map_err(ExecError::Parse)?;
    match normalized {
        crate::backend::parser::NormalizedAlterTableConstraint::ForeignKey(action) => {
            let referenced_relation = catalog
                .lookup_relation_by_oid(action.referenced_relation_oid)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnknownTable(action.referenced_table.clone()))
                })?;
            add_lock_request(
                &mut requests,
                referenced_relation.rel,
                TableLockMode::ShareUpdateExclusive,
            );
        }
        crate::backend::parser::NormalizedAlterTableConstraint::Check(action)
            if !stmt.only && !action.no_inherit =>
        {
            for child_oid in catalog.find_all_inheritors(relation.relation_oid) {
                if child_oid == relation.relation_oid {
                    continue;
                }
                let child_relation =
                    catalog.lookup_relation_by_oid(child_oid).ok_or_else(|| {
                        ExecError::Parse(ParseError::UnknownTable(child_oid.to_string()))
                    })?;
                add_lock_request(
                    &mut requests,
                    child_relation.rel,
                    TableLockMode::AccessExclusive,
                );
            }
        }
        _ => {}
    }

    Ok(requests.into_iter().collect())
}

pub(crate) fn alter_table_validate_constraint_lock_requests(
    relation: &BoundRelation,
    stmt: &AlterTableValidateConstraintStatement,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<TableLockRequest>, ExecError> {
    let mut requests = BTreeMap::new();
    add_lock_request(&mut requests, relation.rel, TableLockMode::AccessExclusive);

    let rows = catalog.constraint_rows_for_relation(relation.relation_oid);
    let row = rows
        .iter()
        .find(|row| row.conname.eq_ignore_ascii_case(&stmt.constraint_name))
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "existing table constraint",
                actual: format!("constraint \"{}\" does not exist", stmt.constraint_name),
            })
        })?;
    if row.contype == CONSTRAINT_FOREIGN {
        let constraints = bind_relation_constraints(
            Some(relation_basename(&stmt.table_name)),
            relation.relation_oid,
            &relation.desc,
            catalog,
        )
        .map_err(ExecError::Parse)?;
        let constraint = constraints
            .foreign_keys
            .iter()
            .find(|constraint| {
                constraint
                    .constraint_name
                    .eq_ignore_ascii_case(&stmt.constraint_name)
            })
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "bound foreign key constraint",
                    actual: format!("missing foreign key binding for {}", stmt.constraint_name),
                })
            })?;
        add_lock_request(
            &mut requests,
            constraint.referenced_rel,
            TableLockMode::ShareUpdateExclusive,
        );
    }

    Ok(requests.into_iter().collect())
}

pub(crate) fn merge_table_lock_requests(
    primary: &[TableLockRequest],
    extra: &[TableLockRequest],
) -> Vec<TableLockRequest> {
    let mut requests = BTreeMap::new();
    for &(rel, mode) in primary {
        add_lock_request(&mut requests, rel, mode);
    }
    for &(rel, mode) in extra {
        add_lock_request(&mut requests, rel, mode);
    }
    requests.into_iter().collect()
}

pub(crate) fn table_lock_relations(requests: &[TableLockRequest]) -> Vec<RelFileLocator> {
    requests.iter().map(|(rel, _)| *rel).collect()
}

#[derive(Debug, Clone)]
enum ConstraintValidationTarget {
    ImmediateOnly,
    All,
    Selected(BTreeSet<u32>),
}

fn build_constraint_validation_context(
    db: &Database,
    client_id: ClientId,
    catalog: &LazyCatalogLookup,
    xid: TransactionId,
    cid: CommandId,
    interrupts: Arc<InterruptState>,
    datetime_config: &DateTimeConfig,
) -> Result<ExecutorContext, ExecError> {
    let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
    Ok(ExecutorContext {
        pool: db.pool.clone(),
        data_dir: None,
        txns: db.txns.clone(),
        txn_waiter: Some(db.txn_waiter.clone()),
        lock_status_provider: Some(Arc::new(db.clone())),
        sequences: Some(db.sequences.clone()),
        large_objects: Some(db.large_objects.clone()),
        stats_import_runtime: None,
        async_notify_runtime: Some(db.async_notify_runtime.clone()),
        advisory_locks: Arc::clone(&db.advisory_locks),
        row_locks: Arc::clone(&db.row_locks),
        checkpoint_stats: db.checkpoint_stats_snapshot(),
        datetime_config: datetime_config.clone(),
        statement_timestamp_usecs:
            crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
        gucs: std::collections::HashMap::new(),
        interrupts,
        stats: Arc::clone(&db.stats),
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
        allow_side_effects: true,
        pending_async_notifications: Vec::new(),
        catalog_effects: Vec::new(),
        temp_effects: Vec::new(),
        database: None,
        pending_catalog_effects: Vec::new(),
        pending_table_locks: Vec::new(),
        catalog: Some(crate::backend::executor::executor_catalog(catalog.clone())),
        scalar_function_cache: std::collections::HashMap::new(),
        plpgsql_function_cache: db.plpgsql_function_cache(client_id),
        pinned_cte_tables: std::collections::HashMap::new(),
        cte_tables: std::collections::HashMap::new(),
        cte_producers: std::collections::HashMap::new(),
        recursive_worktables: std::collections::HashMap::new(),
        deferred_foreign_keys: None,
        trigger_depth: 0,
    })
}

fn should_validate_constraint(
    catalog: &LazyCatalogLookup,
    tracker: &DeferredForeignKeyTracker,
    constraint_oid: u32,
    target: &ConstraintValidationTarget,
) -> bool {
    match target {
        ConstraintValidationTarget::All => true,
        ConstraintValidationTarget::Selected(selected) => selected.contains(&constraint_oid),
        ConstraintValidationTarget::ImmediateOnly => catalog
            .constraint_row_by_oid(constraint_oid)
            .map(|row| {
                tracker.effective_timing(constraint_oid, row.condeferrable, row.condeferred)
                    == ConstraintTiming::Immediate
            })
            .unwrap_or(false),
    }
}

fn collect_constraint_validation_targets(
    catalog: &LazyCatalogLookup,
    tracker: &DeferredForeignKeyTracker,
    target: &ConstraintValidationTarget,
) -> (
    BTreeSet<u32>,
    Vec<PendingParentForeignKeyCheck>,
    BTreeSet<u32>,
) {
    let foreign_keys = tracker
        .affected_constraint_oids()
        .into_iter()
        .filter(|constraint_oid| {
            should_validate_constraint(catalog, tracker, *constraint_oid, target)
        })
        .collect::<BTreeSet<_>>();
    let parent_checks = tracker
        .pending_parent_foreign_key_checks()
        .into_iter()
        .filter(|check| should_validate_constraint(catalog, tracker, check.constraint_oid, target))
        .collect::<Vec<_>>();
    let unique_constraints = tracker
        .pending_unique_constraint_oids()
        .into_iter()
        .filter(|constraint_oid| {
            should_validate_constraint(catalog, tracker, *constraint_oid, target)
        })
        .collect::<BTreeSet<_>>();
    (foreign_keys, parent_checks, unique_constraints)
}

fn validate_foreign_key_constraints(
    catalog: &LazyCatalogLookup,
    constraint_oids: &BTreeSet<u32>,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    for &constraint_oid in constraint_oids {
        let Some(row) = catalog.constraint_row_by_oid(constraint_oid) else {
            continue;
        };
        if row.contype != CONSTRAINT_FOREIGN {
            continue;
        }
        let Some(relation) = catalog.lookup_relation_by_oid(row.conrelid) else {
            continue;
        };
        let constraints =
            bind_relation_constraints(None, relation.relation_oid, &relation.desc, catalog)
                .map_err(ExecError::Parse)?;
        let Some(constraint) = constraints
            .foreign_keys
            .iter()
            .find(|constraint| constraint.constraint_oid == constraint_oid)
        else {
            continue;
        };
        let rows =
            collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, ctx)?;
        for (_, values) in rows {
            enforce_outbound_foreign_keys(
                &constraint.relation_name,
                std::slice::from_ref(constraint),
                None,
                &values,
                ctx,
            )?;
        }
    }
    Ok(())
}

fn validate_parent_foreign_key_checks(
    catalog: &LazyCatalogLookup,
    checks: &[PendingParentForeignKeyCheck],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    for check in checks {
        let Some(row) = catalog.constraint_row_by_oid(check.constraint_oid) else {
            continue;
        };
        if row.contype != CONSTRAINT_FOREIGN {
            continue;
        }
        let Some(referenced_relation) = catalog.lookup_relation_by_oid(row.confrelid) else {
            continue;
        };
        let constraints = bind_referenced_by_foreign_keys(
            referenced_relation.relation_oid,
            &referenced_relation.desc,
            catalog,
        )
        .map_err(ExecError::Parse)?;
        let Some(constraint) = constraints
            .iter()
            .find(|constraint| constraint.constraint_oid == check.constraint_oid)
        else {
            continue;
        };
        enforce_deferred_inbound_foreign_key_check(
            &check.relation_name,
            constraint,
            &check.old_parent_values,
            check.replacement_parent_values.as_deref(),
            ctx,
        )?;
    }
    Ok(())
}

fn validate_unique_constraints(
    catalog: &LazyCatalogLookup,
    tracker: &DeferredForeignKeyTracker,
    constraint_oids: &BTreeSet<u32>,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    for &constraint_oid in constraint_oids {
        let Some(row) = catalog.constraint_row_by_oid(constraint_oid) else {
            continue;
        };
        if !matches!(row.contype, CONSTRAINT_PRIMARY | CONSTRAINT_UNIQUE) {
            continue;
        }
        let Some(relation) = catalog.lookup_relation_by_oid(row.conrelid) else {
            continue;
        };
        let Some(index) = catalog
            .index_relations_for_heap(row.conrelid)
            .into_iter()
            .find(|index| index.relation_oid == row.conindid)
        else {
            continue;
        };
        for pending in tracker.pending_unique_checks(constraint_oid) {
            let insert_ctx = build_immediate_index_insert_context(
                relation.rel,
                &relation.desc,
                &index,
                pending.key_values.clone(),
                pending.heap_tid,
                ctx,
            );
            if probe_unique_conflict(&insert_ctx, &pending.key_values)?.is_some() {
                let key_count = usize::try_from(insert_ctx.index_meta.indnkeyatts.max(0))
                    .unwrap_or_default()
                    .min(insert_ctx.index_desc.columns.len())
                    .min(pending.key_values.len());
                let detail = crate::backend::executor::relation_values_visible_for_error_detail(
                    insert_ctx.index_meta.indrelid,
                    ctx,
                )
                .then(|| {
                    crate::backend::executor::value_io::format_unique_key_detail(
                        &insert_ctx.index_desc.columns[..key_count],
                        &pending.key_values[..key_count],
                    )
                });
                return Err(ExecError::UniqueViolation {
                    constraint: row.conname.clone(),
                    detail,
                });
            }
        }
    }
    Ok(())
}

fn validate_constraint_target(
    db: &Database,
    client_id: ClientId,
    catalog: &LazyCatalogLookup,
    xid: TransactionId,
    cid: CommandId,
    interrupts: Arc<InterruptState>,
    datetime_config: &DateTimeConfig,
    tracker: &DeferredForeignKeyTracker,
    target: ConstraintValidationTarget,
) -> Result<(), ExecError> {
    let (foreign_key_oids, parent_checks, unique_oids) =
        collect_constraint_validation_targets(catalog, tracker, &target);
    if foreign_key_oids.is_empty() && parent_checks.is_empty() && unique_oids.is_empty() {
        return Ok(());
    }
    let mut ctx = build_constraint_validation_context(
        db,
        client_id,
        catalog,
        xid,
        cid,
        interrupts,
        datetime_config,
    )?;
    validate_parent_foreign_key_checks(catalog, &parent_checks, &mut ctx)?;
    validate_foreign_key_constraints(catalog, &foreign_key_oids, &mut ctx)?;
    validate_unique_constraints(catalog, tracker, &unique_oids, &mut ctx)?;
    let parent_foreign_key_oids = parent_checks
        .iter()
        .map(|check| check.constraint_oid)
        .collect::<BTreeSet<_>>();
    tracker.clear_foreign_key_constraints(&foreign_key_oids);
    tracker.clear_parent_foreign_key_checks(&parent_foreign_key_oids);
    tracker.clear_unique_constraints(&unique_oids);
    Ok(())
}

pub(crate) fn validate_immediate_constraints(
    db: &Database,
    client_id: ClientId,
    catalog: &LazyCatalogLookup,
    xid: TransactionId,
    cid: CommandId,
    interrupts: Arc<InterruptState>,
    datetime_config: &DateTimeConfig,
    tracker: &DeferredForeignKeyTracker,
) -> Result<(), ExecError> {
    validate_constraint_target(
        db,
        client_id,
        catalog,
        xid,
        cid,
        interrupts,
        datetime_config,
        tracker,
        ConstraintValidationTarget::ImmediateOnly,
    )
}

pub(crate) fn validate_deferred_constraints(
    db: &Database,
    client_id: ClientId,
    catalog: &LazyCatalogLookup,
    xid: TransactionId,
    cid: CommandId,
    interrupts: Arc<InterruptState>,
    datetime_config: &DateTimeConfig,
    tracker: &DeferredForeignKeyTracker,
) -> Result<(), ExecError> {
    validate_constraint_target(
        db,
        client_id,
        catalog,
        xid,
        cid,
        interrupts,
        datetime_config,
        tracker,
        ConstraintValidationTarget::All,
    )
}

pub(crate) fn validate_deferred_foreign_key_constraints(
    db: &Database,
    client_id: ClientId,
    catalog: &LazyCatalogLookup,
    xid: TransactionId,
    cid: CommandId,
    interrupts: Arc<InterruptState>,
    datetime_config: &DateTimeConfig,
    tracker: &DeferredForeignKeyTracker,
) -> Result<(), ExecError> {
    validate_deferred_constraints(
        db,
        client_id,
        catalog,
        xid,
        cid,
        interrupts,
        datetime_config,
        tracker,
    )
}

fn qualified_constraint_name(name: &QualifiedNameRef) -> String {
    match &name.schema_name {
        Some(schema_name) => format!("{schema_name}.{}", name.name),
        None => name.name.clone(),
    }
}

fn matched_constraint_roots_for_name(
    catalog: &LazyCatalogLookup,
    all_rows: &[crate::include::catalog::PgConstraintRow],
    namespace_names: &HashMap<u32, String>,
    name: &QualifiedNameRef,
) -> Vec<crate::include::catalog::PgConstraintRow> {
    let matches_in_schema = |schema_name: &str| {
        all_rows
            .iter()
            .filter(|row| {
                row.conname.eq_ignore_ascii_case(&name.name)
                    && namespace_names
                        .get(&row.connamespace)
                        .is_some_and(|namespace| namespace.eq_ignore_ascii_case(schema_name))
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    if let Some(schema_name) = &name.schema_name {
        return matches_in_schema(schema_name);
    }
    for schema_name in catalog.search_path() {
        let matches = matches_in_schema(&schema_name);
        if !matches.is_empty() {
            return matches;
        }
    }
    Vec::new()
}

fn collect_constraint_descendants(
    by_parent: &HashMap<u32, Vec<crate::include::catalog::PgConstraintRow>>,
    parent_oid: u32,
    target: &mut BTreeSet<u32>,
) {
    let Some(children) = by_parent.get(&parent_oid) else {
        return;
    };
    for child in children {
        if target.insert(child.oid) {
            collect_constraint_descendants(by_parent, child.oid, target);
        }
    }
}

fn resolve_set_constraints_targets(
    catalog: &LazyCatalogLookup,
    constraints: &[QualifiedNameRef],
) -> Result<BTreeSet<u32>, ExecError> {
    let all_rows = catalog.constraint_rows();
    let namespace_names = catalog
        .namespace_rows()
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<HashMap<_, _>>();
    let mut by_parent = HashMap::<u32, Vec<crate::include::catalog::PgConstraintRow>>::new();
    for row in &all_rows {
        if row.conparentid != 0 {
            by_parent
                .entry(row.conparentid)
                .or_default()
                .push(row.clone());
        }
    }
    let mut target_oids = BTreeSet::new();
    for name in constraints {
        let matched_roots =
            matched_constraint_roots_for_name(catalog, &all_rows, &namespace_names, name);
        if matched_roots.is_empty() {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "existing constraint name",
                actual: format!(
                    "constraint \"{}\" does not exist",
                    qualified_constraint_name(name)
                ),
            }));
        }
        for row in matched_roots {
            target_oids.insert(row.oid);
            collect_constraint_descendants(&by_parent, row.oid, &mut target_oids);
        }
    }
    Ok(target_oids)
}

pub(crate) fn execute_set_constraints(
    db: &Database,
    client_id: ClientId,
    catalog: &LazyCatalogLookup,
    xid: Option<TransactionId>,
    cid: CommandId,
    interrupts: Arc<InterruptState>,
    datetime_config: &DateTimeConfig,
    tracker: &DeferredForeignKeyTracker,
    stmt: &SetConstraintsStatement,
) -> Result<StatementResult, ExecError> {
    let desired_timing = if stmt.deferred {
        ConstraintTiming::Deferred
    } else {
        ConstraintTiming::Immediate
    };
    let target_oids = if let Some(constraints) = &stmt.constraints {
        let target_oids = resolve_set_constraints_targets(catalog, constraints)?;
        if stmt.deferred {
            if let Some(row) = target_oids.iter().find_map(|constraint_oid| {
                catalog
                    .constraint_row_by_oid(*constraint_oid)
                    .filter(|row| !row.condeferrable)
            }) {
                return Err(ExecError::DetailedError {
                    message: format!("constraint \"{}\" is not deferrable", row.conname),
                    detail: None,
                    hint: None,
                    sqlstate: "55000",
                });
            }
        }
        Some(target_oids)
    } else {
        None
    };

    if desired_timing == ConstraintTiming::Immediate {
        if let Some(xid) = xid {
            match &target_oids {
                Some(target_oids) if !target_oids.is_empty() => validate_constraint_target(
                    db,
                    client_id,
                    catalog,
                    xid,
                    cid,
                    Arc::clone(&interrupts),
                    datetime_config,
                    tracker,
                    ConstraintValidationTarget::Selected(target_oids.clone()),
                )?,
                None => validate_deferred_constraints(
                    db,
                    client_id,
                    catalog,
                    xid,
                    cid,
                    Arc::clone(&interrupts),
                    datetime_config,
                    tracker,
                )?,
                _ => {}
            }
        }
    }

    match target_oids {
        Some(target_oids) => {
            for constraint_oid in target_oids {
                tracker.set_constraint_timing(constraint_oid, desired_timing);
            }
        }
        None => tracker.set_all_timing(desired_timing),
    }

    Ok(StatementResult::AffectedRows(0))
}

fn add_relation_foreign_key_partner_locks(
    requests: &mut BTreeMap<RelFileLocator, TableLockMode>,
    constraints: &BoundRelationConstraints,
) {
    for constraint in &constraints.foreign_keys {
        add_lock_request(
            requests,
            constraint.referenced_rel,
            TableLockMode::ShareUpdateExclusive,
        );
    }
}

fn add_lock_request(
    requests: &mut BTreeMap<RelFileLocator, TableLockMode>,
    rel: RelFileLocator,
    mode: TableLockMode,
) {
    requests
        .entry(rel)
        .and_modify(|existing| *existing = existing.strongest(mode))
        .or_insert(mode);
}

fn relation_basename(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}
