use std::collections::BTreeMap;
use std::sync::Arc;

use crate::ClientId;
use crate::backend::access::transam::xact::{CommandId, TransactionId};
use crate::backend::commands::tablecmds::collect_matching_rows_heap;
use crate::backend::executor::{
    DeferredForeignKeyTracker, ExecError, ExecutorContext, enforce_outbound_foreign_keys,
};
use crate::backend::parser::{
    AlterTableAddConstraintStatement, AlterTableValidateConstraintStatement, BoundDeleteStatement,
    BoundInsertStatement, BoundRelation, BoundRelationConstraints, BoundUpdateStatement,
    CatalogLookup, ParseError, PreparedInsert, bind_relation_constraints,
    normalize_alter_table_add_constraint,
};
use crate::backend::storage::lmgr::TableLockMode;
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::utils::cache::lsyscache::LazyCatalogLookup;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::misc::interrupts::InterruptState;
use crate::include::catalog::CONSTRAINT_FOREIGN;

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
    if let crate::backend::parser::NormalizedAlterTableConstraint::ForeignKey(action) = normalized {
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

pub(crate) fn validate_deferred_foreign_key_constraints(
    db: &Database,
    client_id: ClientId,
    catalog: &LazyCatalogLookup<'_>,
    xid: TransactionId,
    cid: CommandId,
    interrupts: Arc<InterruptState>,
    datetime_config: &DateTimeConfig,
    tracker: &DeferredForeignKeyTracker,
) -> Result<(), ExecError> {
    let affected_constraint_oids = tracker.affected_constraint_oids();
    if affected_constraint_oids.is_empty() {
        return Ok(());
    }

    let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
    let mut ctx = ExecutorContext {
        pool: db.pool.clone(),
        txns: db.txns.clone(),
        txn_waiter: Some(db.txn_waiter.clone()),
        sequences: Some(db.sequences.clone()),
        large_objects: Some(db.large_objects.clone()),
        async_notify_runtime: Some(db.async_notify_runtime.clone()),
        advisory_locks: Arc::clone(&db.advisory_locks),
        row_locks: Arc::clone(&db.row_locks),
        checkpoint_stats: db.checkpoint_stats_snapshot(),
        datetime_config: datetime_config.clone(),
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
        expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
        case_test_values: Vec::new(),
        system_bindings: Vec::new(),
        subplans: Vec::new(),
        timed: false,
        allow_side_effects: true,
        pending_async_notifications: Vec::new(),
        catalog: catalog.materialize_visible_catalog(),
        compiled_functions: std::collections::HashMap::new(),
        cte_tables: std::collections::HashMap::new(),
        cte_producers: std::collections::HashMap::new(),
        recursive_worktables: std::collections::HashMap::new(),
        deferred_foreign_keys: None,
        trigger_depth: 0,
    };

    for constraint_oid in affected_constraint_oids {
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
        let rows = collect_matching_rows_heap(
            relation.rel,
            &relation.desc,
            relation.toast,
            None,
            &mut ctx,
        )?;
        for (_, values) in rows {
            enforce_outbound_foreign_keys(
                &constraint.relation_name,
                std::slice::from_ref(constraint),
                None,
                &values,
                &mut ctx,
            )?;
        }
    }

    Ok(())
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
