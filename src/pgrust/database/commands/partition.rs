use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use super::super::*;
use super::typed_table::reject_typed_table_ddl;
use crate::backend::commands::partition::{
    validate_attach_partition_constraints, validate_default_partition_rows_for_new_bound,
    validate_new_partition_bound, validate_partition_relation_compatibility,
    validate_relation_rows_for_partition_bound,
};
use crate::backend::commands::tablecmds::{
    PublicationDmlAction, enforce_publication_replica_identity,
};
use crate::backend::executor::ExecutorContext;
use crate::backend::executor::exec_expr::partition_constraint_conditions_for_catalog;
use crate::backend::parser::{
    AlterTableAttachPartitionStatement, AlterTableDetachPartitionStatement, BoundRelation,
    CatalogLookup, DetachPartitionMode, deserialize_partition_bound,
    lower_partition_bound_for_relation, serialize_partition_bound,
};
use crate::backend::storage::lmgr::{TableLockMode, lock_table_requests_interruptible};
use crate::backend::utils::cache::syscache::{
    SearchSysCacheList1, SysCacheId, SysCacheTuple, oid_key,
};
use crate::backend::utils::misc::interrupts::check_for_interrupts;
use crate::include::catalog::{
    CONSTRAINT_CHECK, CONSTRAINT_FOREIGN, CONSTRAINT_NOTNULL, CONSTRAINT_PRIMARY,
    CONSTRAINT_UNIQUE, PgInheritsRow,
};

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

fn relation_name_for_oid(catalog: &dyn CatalogLookup, relation_oid: u32) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation_oid.to_string())
}

fn relation_basename(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

fn validate_attached_partition_publication_replica_identity(
    catalog: &dyn CatalogLookup,
    child: &BoundRelation,
    partition_name: &str,
) -> Result<(), ExecError> {
    let indexes = catalog.index_relations_for_heap(child.relation_oid);
    for action in [PublicationDmlAction::Update, PublicationDmlAction::Delete] {
        enforce_publication_replica_identity(
            relation_basename(partition_name),
            child.relation_oid,
            child.namespace_oid,
            &child.desc,
            &indexes,
            catalog,
            action,
            true,
        )?;
    }
    Ok(())
}

fn partition_subtree_oids(catalog: &dyn CatalogLookup, root_oid: u32) -> Vec<u32> {
    let mut subtree = vec![root_oid];
    let mut stack = vec![root_oid];
    while let Some(parent_oid) = stack.pop() {
        for child in catalog.inheritance_children(parent_oid) {
            subtree.push(child.inhrelid);
            stack.push(child.inhrelid);
        }
    }
    subtree
}

fn reject_attach_partition_referenced_by_foreign_key(
    catalog: &dyn CatalogLookup,
    child: &BoundRelation,
    partition_name: &str,
) -> Result<(), ExecError> {
    let constraints = catalog.constraint_rows();
    for referenced_oid in partition_subtree_oids(catalog, child.relation_oid) {
        if let Some(row) = constraints
            .iter()
            .find(|row| row.contype == CONSTRAINT_FOREIGN && row.confrelid == referenced_oid)
        {
            let referenced_name = relation_name_for_oid(catalog, referenced_oid);
            if referenced_oid == child.relation_oid {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "cannot attach table \"{}\" as a partition because it is referenced by foreign key \"{}\"",
                        relation_basename(partition_name),
                        row.conname
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "55000",
                });
            }
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot ALTER TABLE \"{}\" because it is being used by active queries in this session",
                    referenced_name
                ),
                detail: None,
                hint: None,
                sqlstate: "55006",
            });
        }
    }
    Ok(())
}

fn lookup_partition_alter_parent(
    catalog: &dyn CatalogLookup,
    table_name: &str,
    if_exists: bool,
    unsupported_action: &'static str,
    partitioned_index_action: Option<&'static str>,
) -> Result<Option<BoundRelation>, ExecError> {
    let Some(parent) = catalog.lookup_any_relation(table_name) else {
        if if_exists {
            return Ok(None);
        }
        return Err(ExecError::Parse(ParseError::TableDoesNotExist(
            table_name.to_string(),
        )));
    };
    if parent.relkind == 'I'
        && let Some(action) = partitioned_index_action
    {
        return Err(ExecError::DetailedError {
            message: format!(
                "ALTER action {action} cannot be performed on relation \"{table_name}\""
            ),
            detail: Some("This operation is not supported for partitioned indexes.".into()),
            hint: None,
            sqlstate: "42809",
        });
    }
    if unsupported_action == "ATTACH PARTITION"
        && (matches!(parent.relkind, 'i' | 'I')
            || catalog.index_row_by_oid(parent.relation_oid).is_some())
    {
        let relation_name = relation_name_for_oid(catalog, parent.relation_oid);
        return Err(ExecError::DetailedError {
            message: format!("\"{relation_name}\" is not a partitioned table"),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    if parent.relkind != 'p' || parent.partitioned_table.is_none() {
        let relation_name = relation_name_for_oid(catalog, parent.relation_oid);
        return Err(ExecError::DetailedError {
            message: format!(
                "ALTER action {unsupported_action} cannot be performed on relation \"{relation_name}\""
            ),
            detail: Some("This operation is not supported for tables.".into()),
            hint: None,
            sqlstate: "42809",
        });
    }
    Ok(Some(parent))
}

fn lookup_partition_alter_child(
    catalog: &dyn CatalogLookup,
    table_name: &str,
) -> Result<BoundRelation, ExecError> {
    catalog
        .lookup_any_relation(table_name)
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(table_name.to_string())))
}

fn reject_attach_foreign_partition_with_unique_parent_indexes(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    parent: &BoundRelation,
    parent_name: &str,
    child: &BoundRelation,
    child_name: &str,
) -> Result<(), ExecError> {
    if child.relkind != 'f' {
        return Ok(());
    }
    let has_unique_index = SearchSysCacheList1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::INDEXINDRELID,
        oid_key(parent.relation_oid),
    )
    .map_err(map_catalog_error)?
    .into_iter()
    .filter_map(|tuple| match tuple {
        SysCacheTuple::Index(row) => Some(row),
        _ => None,
    })
    .any(|row| row.indisunique || row.indisprimary);
    if !has_unique_index {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!(
            "cannot attach foreign table \"{child_name}\" as partition of partitioned table \"{parent_name}\""
        ),
        detail: Some(format!(
            "Partitioned table \"{parent_name}\" contains unique indexes."
        )),
        hint: None,
        sqlstate: "42809",
    })
}

fn detach_unsupported_action(mode: &DetachPartitionMode) -> &'static str {
    match mode {
        DetachPartitionMode::Finalize => "DETACH PARTITION ... FINALIZE",
        DetachPartitionMode::Immediate | DetachPartitionMode::Concurrently => "DETACH PARTITION",
    }
}

fn reject_typed_attach_partition_child(child: &BoundRelation) -> Result<(), ExecError> {
    if child.of_type_oid == 0 {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: "cannot attach a typed table as partition".into(),
        detail: None,
        hint: None,
        sqlstate: "42809",
    })
}

fn column_attnum_by_name(relation: &BoundRelation, column_name: &str) -> Option<i16> {
    relation
        .desc
        .columns
        .iter()
        .enumerate()
        .find_map(|(index, column)| {
            (!column.dropped && column.name.eq_ignore_ascii_case(column_name))
                .then_some(index.saturating_add(1) as i16)
        })
}

fn not_null_constraint_for_attnum(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    attnum: i16,
) -> Option<crate::include::catalog::PgConstraintRow> {
    catalog
        .constraint_rows_for_relation(relation_oid)
        .into_iter()
        .find(|row| {
            row.contype == CONSTRAINT_NOTNULL
                && row
                    .conkey
                    .as_ref()
                    .is_some_and(|keys| keys.contains(&attnum))
        })
}

fn partition_inheritance_row(
    catalog: &dyn CatalogLookup,
    child_oid: u32,
    parent_oid: u32,
) -> Option<PgInheritsRow> {
    catalog
        .inheritance_parents(child_oid)
        .into_iter()
        .find(|row| row.inhparent == parent_oid)
}

fn validate_attach_partition_not_circular(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    child: &BoundRelation,
) -> Result<(), ExecError> {
    if catalog
        .find_all_inheritors(child.relation_oid)
        .contains(&parent.relation_oid)
    {
        return Err(ExecError::DetailedError {
            message: "circular inheritance not allowed".into(),
            detail: Some(format!(
                "\"{}\" is already a child of \"{}\".",
                relation_name_for_oid(catalog, parent.relation_oid),
                relation_name_for_oid(catalog, child.relation_oid),
            )),
            hint: None,
            sqlstate: "42P17",
        });
    }
    Ok(())
}

fn validate_detach_inheritance_state(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    child: &BoundRelation,
    stmt: &AlterTableDetachPartitionStatement,
    expect_pending: Option<bool>,
) -> Result<PgInheritsRow, ExecError> {
    let Some(row) = partition_inheritance_row(catalog, child.relation_oid, parent.relation_oid)
    else {
        return Err(ExecError::DetailedError {
            message: format!(
                "relation \"{}\" is not a partition of relation \"{}\"",
                relation_name_for_oid(catalog, child.relation_oid),
                relation_name_for_oid(catalog, parent.relation_oid)
            ),
            detail: None,
            hint: None,
            sqlstate: "42P01",
        });
    };
    if let Some(true) = expect_pending
        && !row.inhdetachpending
    {
        return Err(ExecError::DetailedError {
            message: format!(
                "partition \"{}\" of relation \"{}\" is not pending detach",
                stmt.partition_table, stmt.parent_table
            ),
            detail: None,
            hint: None,
            sqlstate: "55000",
        });
    }
    if let Some(false) = expect_pending
        && row.inhdetachpending
    {
        return Err(ExecError::DetailedError {
            message: format!(
                "partition \"{}\" of relation \"{}\" is pending detach",
                stmt.partition_table, stmt.parent_table
            ),
            detail: None,
            hint: Some(format!(
                "Use ALTER TABLE {} DETACH PARTITION {} FINALIZE to complete the pending detach operation.",
                stmt.parent_table, stmt.partition_table
            )),
            sqlstate: "55000",
        });
    }
    if !child.relispartition {
        return Err(ExecError::DetailedError {
            message: format!("table \"{}\" is not a partition", stmt.partition_table),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    Ok(row)
}

fn unwrap_constraint_condition(condition: String) -> String {
    if !condition.starts_with('(') || !condition.ends_with(')') {
        return condition;
    }
    let mut depth = 0i32;
    for (index, ch) in condition.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 && index + ch.len_utf8() < condition.len() {
                    return condition;
                }
            }
            _ => {}
        }
    }
    condition[1..condition.len() - 1].to_string()
}

fn detached_partition_bound_check(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    child: &BoundRelation,
) -> Result<Option<(String, String)>, ExecError> {
    let Some(bound_text) = child.relpartbound.as_deref() else {
        return Ok(None);
    };
    let Some(partitioned) = parent.partitioned_table.as_ref() else {
        return Ok(None);
    };
    let Some(first_attnum) = partitioned
        .partattrs
        .first()
        .copied()
        .filter(|attnum| *attnum > 0)
    else {
        return Ok(None);
    };
    let Some(parent_column) = parent
        .desc
        .columns
        .get(first_attnum.saturating_sub(1) as usize)
        .filter(|column| !column.dropped)
    else {
        return Ok(None);
    };
    let child_column_name = child
        .desc
        .columns
        .iter()
        .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(&parent_column.name))
        .map(|column| column.name.clone())
        .unwrap_or_else(|| parent_column.name.clone());
    let child_name = relation_name_for_oid(catalog, child.relation_oid);
    let constraint_name = format!("{child_name}_{child_column_name}_check");
    if catalog
        .constraint_rows_for_relation(child.relation_oid)
        .into_iter()
        .any(|row| row.conname.eq_ignore_ascii_case(&constraint_name))
    {
        return Ok(None);
    }
    let bound = deserialize_partition_bound(bound_text).map_err(ExecError::Parse)?;
    let Some(conditions) = partition_constraint_conditions_for_catalog(catalog, parent, &bound)?
    else {
        return Ok(None);
    };
    if conditions.is_empty() {
        return Ok(None);
    }
    let expr_sql = conditions
        .into_iter()
        .map(unwrap_constraint_condition)
        .collect::<Vec<_>>()
        .join(" AND ");
    Ok(Some((constraint_name, expr_sql)))
}

fn detach_lock_requests(
    parent: &BoundRelation,
    child: &BoundRelation,
    parent_mode: TableLockMode,
    child_mode: TableLockMode,
) -> Vec<(RelFileLocator, TableLockMode)> {
    let mut requests = BTreeMap::new();
    add_lock_request(&mut requests, parent.rel, parent_mode);
    add_lock_request(&mut requests, child.rel, child_mode);
    requests.into_iter().collect()
}

fn ddl_executor_context(
    db: &Database,
    catalog: crate::backend::utils::cache::lsyscache::LazyCatalogLookup,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
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
        datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        statement_timestamp_usecs:
            crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
        gucs: std::collections::HashMap::new(),
        interrupts,
        stats: std::sync::Arc::clone(&db.stats),
        session_stats: db.session_stats_state(client_id),
        snapshot,
        write_xid_override: None,
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
        security_restricted: false,
        pending_async_notifications: Vec::new(),
        catalog_effects: Vec::new(),
        temp_effects: Vec::new(),
        database: Some(db.clone()),
        pending_catalog_effects: Vec::new(),
        pending_table_locks: Vec::new(),
        pending_portals: Vec::new(),
        catalog: Some(crate::backend::executor::executor_catalog(catalog)),
        scalar_function_cache: std::collections::HashMap::new(),
        proc_execute_acl_cache: std::collections::HashSet::new(),
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

impl Database {
    pub(crate) fn execute_alter_table_attach_partition_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTableAttachPartitionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(parent) = lookup_partition_alter_parent(
            &catalog,
            &stmt.parent_table,
            stmt.if_exists,
            "ATTACH PARTITION",
            None,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let child = lookup_partition_alter_child(&catalog, &stmt.partition_table)?;
        let mut requests = BTreeMap::new();
        add_lock_request(&mut requests, parent.rel, TableLockMode::AccessExclusive);
        add_lock_request(&mut requests, child.rel, TableLockMode::AccessExclusive);
        if let Some(partitioned_table) = parent.partitioned_table.as_ref()
            && partitioned_table.partdefid != 0
            && let Some(default_partition) = catalog.relation_by_oid(partitioned_table.partdefid)
        {
            add_lock_request(
                &mut requests,
                default_partition.rel,
                TableLockMode::AccessExclusive,
            );
        }
        let requests = requests.into_iter().collect::<Vec<_>>();
        lock_table_requests_interruptible(
            &self.table_locks,
            client_id,
            &requests,
            interrupts.as_ref(),
        )?;

        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self
            .execute_alter_table_attach_partition_stmt_in_transaction_with_search_path(
                client_id,
                stmt,
                xid,
                0,
                configured_search_path,
                &mut catalog_effects,
            );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        for (rel, _) in requests {
            self.table_locks.unlock_table(rel, client_id);
        }
        result
    }

    pub(crate) fn execute_alter_table_attach_partition_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTableAttachPartitionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(parent) = lookup_partition_alter_parent(
            &catalog,
            &stmt.parent_table,
            stmt.if_exists,
            "ATTACH PARTITION",
            None,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let child = lookup_partition_alter_child(&catalog, &stmt.partition_table)?;
        ensure_relation_owner(self, client_id, &parent, &stmt.parent_table)?;
        ensure_relation_owner(self, client_id, &child, &stmt.partition_table)?;
        reject_typed_table_ddl(&parent, "attach partition to")?;
        reject_typed_attach_partition_child(&child)?;
        if let Some(column) = child
            .desc
            .columns
            .iter()
            .find(|column| !column.dropped && column.identity.is_some())
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "table \"{}\" being attached contains an identity column \"{}\"",
                    stmt.partition_table, column.name
                ),
                detail: Some("The new partition may not contain an identity column.".into()),
                hint: None,
                sqlstate: "42809",
            });
        }
        reject_attach_foreign_partition_with_unique_parent_indexes(
            self,
            client_id,
            Some((xid, cid)),
            &parent,
            &stmt.parent_table,
            &child,
            &stmt.partition_table,
        )?;
        validate_partition_relation_compatibility(
            &catalog,
            &parent,
            &stmt.parent_table,
            &child,
            &stmt.partition_table,
        )?;
        reject_attach_partition_referenced_by_foreign_key(&catalog, &child, &stmt.partition_table)?;
        validate_attach_partition_not_circular(&catalog, &parent, &child)?;

        let bound = lower_partition_bound_for_relation(&parent, &stmt.bound, &catalog)
            .map_err(ExecError::Parse)?;
        let mut ctx = ddl_executor_context(
            self,
            catalog.clone(),
            client_id,
            xid,
            cid.saturating_add(1),
            std::sync::Arc::clone(&interrupts),
        )?;
        if child.relkind != 'f' {
            validate_relation_rows_for_partition_bound(
                &catalog, &parent, &child, &bound, &mut ctx,
            )?;
        }
        validate_default_partition_rows_for_new_bound(&catalog, &parent, &bound, &mut ctx)?;
        validate_new_partition_bound(
            &catalog,
            &parent,
            &stmt.partition_table,
            &bound,
            Some(child.relation_oid),
        )?;
        validate_attach_partition_constraints(&catalog, &parent, &child)?;

        let inherit_ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: cid.saturating_add(2),
            client_id,
            waiter: None,
            interrupts: std::sync::Arc::clone(&interrupts),
        };
        let inherit_effect = self
            .catalog
            .write()
            .create_relation_inheritance_mvcc(
                child.relation_oid,
                &[parent.relation_oid],
                &inherit_ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&inherit_effect)?;
        catalog_effects.push(inherit_effect);

        let next_cid = self.mark_attached_partition_inheritance_metadata_in_transaction(
            client_id,
            xid,
            cid.saturating_add(3),
            parent.relation_oid,
            child.relation_oid,
            configured_search_path,
            catalog_effects,
        )?;
        let relpartbound = Some(serialize_partition_bound(&bound).map_err(ExecError::Parse)?);
        let updated_child = self.replace_relation_partition_metadata_in_transaction(
            client_id,
            child.relation_oid,
            true,
            relpartbound,
            child.partitioned_table.clone(),
            xid,
            next_cid,
            configured_search_path,
            catalog_effects,
        )?;
        let next_cid = next_cid.saturating_add(1);
        let mut next_cid = self.copy_parent_identity_columns_to_attached_partition_in_transaction(
            client_id,
            xid,
            next_cid,
            &parent,
            &updated_child,
            catalog_effects,
        )?;
        if bound.is_default() {
            self.update_partitioned_table_default_partition_in_transaction(
                client_id,
                parent.relation_oid,
                updated_child.relation_oid,
                xid,
                next_cid,
                configured_search_path,
                catalog_effects,
            )?;
            next_cid = next_cid.saturating_add(1);
        }
        let next_cid = self.reconcile_partitioned_parent_keys_for_attached_child_in_transaction(
            client_id,
            xid,
            next_cid.saturating_add(1),
            parent.relation_oid,
            updated_child.relation_oid,
            configured_search_path,
            catalog_effects,
        )?;
        let next_cid = if updated_child.relkind == 'f' {
            next_cid
        } else {
            self.reconcile_partitioned_parent_indexes_for_attached_child_in_transaction(
                client_id,
                xid,
                next_cid,
                parent.relation_oid,
                updated_child.relation_oid,
                configured_search_path,
                catalog_effects,
            )?
        };
        let validation_catalog =
            self.lazy_catalog_lookup(client_id, Some((xid, next_cid)), configured_search_path);
        let publication_child = validation_catalog
            .relation_by_oid(updated_child.relation_oid)
            .unwrap_or_else(|| updated_child.clone());
        validate_attached_partition_publication_replica_identity(
            &validation_catalog,
            &publication_child,
            &stmt.partition_table,
        )?;
        let next_cid = self
            .reconcile_partitioned_parent_foreign_keys_for_attached_child_in_transaction(
                client_id,
                xid,
                next_cid,
                parent.relation_oid,
                updated_child.relation_oid,
                configured_search_path,
                catalog_effects,
            )?;
        self.clone_parent_row_triggers_to_partition_in_transaction(
            client_id,
            xid,
            next_cid,
            parent.relation_oid,
            updated_child.relation_oid,
            configured_search_path,
            catalog_effects,
        )?;
        Ok(StatementResult::AffectedRows(0))
    }

    fn copy_parent_identity_columns_to_attached_partition_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        mut cid: CommandId,
        parent: &BoundRelation,
        child: &BoundRelation,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        for parent_column in parent
            .desc
            .columns
            .iter()
            .filter(|column| !column.dropped && column.identity.is_some())
        {
            let Some(child_column) = child
                .desc
                .columns
                .iter()
                .find(|column| {
                    !column.dropped && column.name.eq_ignore_ascii_case(&parent_column.name)
                })
                .cloned()
            else {
                continue;
            };
            let child_column_name = child_column.name.clone();
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
                .alter_table_set_column_identity_mvcc(
                    child.relation_oid,
                    &child_column_name,
                    parent_column.identity,
                    parent_column.default_expr.clone(),
                    parent_column.default_sequence_oid,
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            cid = cid.saturating_add(1);
        }
        Ok(cid)
    }

    fn mark_attached_partition_inheritance_metadata_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        mut cid: CommandId,
        parent_oid: u32,
        child_oid: u32,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let parent = catalog
            .relation_by_oid(parent_oid)
            .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(parent_oid.to_string())))?;
        let child = catalog
            .relation_by_oid(child_oid)
            .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(child_oid.to_string())))?;

        for parent_column in parent.desc.columns.iter().filter(|column| !column.dropped) {
            let Some(child_attnum) = column_attnum_by_name(&child, &parent_column.name) else {
                continue;
            };
            let child_not_null =
                if parent_column.storage.nullable || parent_column.not_null_constraint_no_inherit {
                    None
                } else {
                    not_null_constraint_for_attnum(&catalog, child.relation_oid, child_attnum)
                        .map(|_| (Some(1), Some(false)))
                };
            let (not_null_inhcount, not_null_is_local) = child_not_null.unwrap_or((None, None));
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
                .update_relation_column_inheritance_mvcc(
                    child.relation_oid,
                    &parent_column.name,
                    1,
                    false,
                    not_null_inhcount,
                    not_null_is_local,
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            cid = cid.saturating_add(1);
        }

        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let child_check_constraints = catalog
            .constraint_rows_for_relation(child.relation_oid)
            .into_iter()
            .filter(|row| row.contype == CONSTRAINT_CHECK)
            .collect::<Vec<_>>();
        for parent_constraint in catalog
            .constraint_rows_for_relation(parent.relation_oid)
            .into_iter()
            .filter(|row| row.contype == CONSTRAINT_CHECK && !row.connoinherit)
        {
            let Some(child_constraint) = child_check_constraints
                .iter()
                .find(|row| row.conname.eq_ignore_ascii_case(&parent_constraint.conname))
            else {
                continue;
            };
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
                .update_check_constraint_inheritance_mvcc(
                    child.relation_oid,
                    child_constraint.oid,
                    parent_constraint.oid,
                    false,
                    1,
                    false,
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            cid = cid.saturating_add(1);
        }

        Ok(cid)
    }

    pub(crate) fn execute_alter_table_detach_partition_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTableDetachPartitionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        if stmt.mode == DetachPartitionMode::Concurrently {
            return self.execute_alter_table_detach_partition_concurrently_with_search_path(
                client_id,
                stmt,
                configured_search_path,
            );
        }

        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(parent) = lookup_partition_alter_parent(
            &catalog,
            &stmt.parent_table,
            stmt.if_exists,
            detach_unsupported_action(&stmt.mode),
            Some("DETACH PARTITION"),
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let child = lookup_partition_alter_child(&catalog, &stmt.partition_table)?;
        validate_detach_inheritance_state(
            &catalog,
            &parent,
            &child,
            stmt,
            Some(stmt.mode == DetachPartitionMode::Finalize),
        )?;
        let requests = detach_lock_requests(
            &parent,
            &child,
            TableLockMode::AccessExclusive,
            TableLockMode::AccessExclusive,
        );
        lock_table_requests_interruptible(
            &self.table_locks,
            client_id,
            &requests,
            interrupts.as_ref(),
        )?;

        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self
            .execute_alter_table_detach_partition_stmt_in_transaction_with_search_path(
                client_id,
                stmt,
                xid,
                0,
                configured_search_path,
                &mut catalog_effects,
            );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        for (rel, _) in requests {
            self.table_locks.unlock_table(rel, client_id);
        }
        result
    }

    pub(crate) fn execute_alter_table_detach_partition_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTableDetachPartitionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        match stmt.mode {
            DetachPartitionMode::Concurrently => Err(ExecError::Parse(
                ParseError::ActiveSqlTransaction("ALTER TABLE ... DETACH CONCURRENTLY"),
            )),
            DetachPartitionMode::Immediate => self.finalize_detach_partition_in_transaction(
                client_id,
                stmt,
                xid,
                cid,
                configured_search_path,
                catalog_effects,
                false,
            ),
            DetachPartitionMode::Finalize => self.finalize_detach_partition_in_transaction(
                client_id,
                stmt,
                xid,
                cid,
                configured_search_path,
                catalog_effects,
                true,
            ),
        }
    }

    fn execute_alter_table_detach_partition_concurrently_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTableDetachPartitionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(parent) = lookup_partition_alter_parent(
            &catalog,
            &stmt.parent_table,
            stmt.if_exists,
            detach_unsupported_action(&stmt.mode),
            Some("DETACH PARTITION"),
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let child = lookup_partition_alter_child(&catalog, &stmt.partition_table)?;
        validate_detach_inheritance_state(&catalog, &parent, &child, stmt, Some(false))?;
        if parent.partitioned_table.as_ref().is_some_and(|row| {
            row.partdefid != 0 && catalog.relation_by_oid(row.partdefid).is_some()
        }) {
            return Err(ExecError::DetailedError {
                message: "cannot detach partitions concurrently when a default partition exists"
                    .into(),
                detail: None,
                hint: None,
                sqlstate: "55000",
            });
        }

        let phase1_requests = detach_lock_requests(
            &parent,
            &child,
            TableLockMode::ShareUpdateExclusive,
            TableLockMode::AccessExclusive,
        );
        lock_table_requests_interruptible(
            &self.table_locks,
            client_id,
            &phase1_requests,
            interrupts.as_ref(),
        )?;
        let phase1_xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, phase1_xid);
        let mut phase1_effects = Vec::new();
        let result = self.mark_detach_partition_pending_in_transaction(
            client_id,
            stmt,
            phase1_xid,
            0,
            configured_search_path,
            &mut phase1_effects,
        );
        let phase1_result =
            self.finish_txn(client_id, phase1_xid, result, &phase1_effects, &[], &[]);
        guard.disarm();
        for (rel, _) in phase1_requests {
            self.table_locks.unlock_table(rel, client_id);
        }
        phase1_result?;

        self.wait_for_detach_phase_one_users(phase1_xid, Arc::clone(&interrupts))?;

        let finalize_stmt = AlterTableDetachPartitionStatement {
            mode: DetachPartitionMode::Finalize,
            ..stmt.clone()
        };
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(parent) = lookup_partition_alter_parent(
            &catalog,
            &finalize_stmt.parent_table,
            finalize_stmt.if_exists,
            detach_unsupported_action(&finalize_stmt.mode),
            Some("DETACH PARTITION"),
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let child = lookup_partition_alter_child(&catalog, &finalize_stmt.partition_table)?;
        validate_detach_inheritance_state(&catalog, &parent, &child, &finalize_stmt, Some(true))?;
        let phase2_requests = detach_lock_requests(
            &parent,
            &child,
            TableLockMode::ShareUpdateExclusive,
            TableLockMode::AccessExclusive,
        );
        lock_table_requests_interruptible(
            &self.table_locks,
            client_id,
            &phase2_requests,
            interrupts.as_ref(),
        )?;
        let phase2_xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, phase2_xid);
        let mut phase2_effects = Vec::new();
        let result = self
            .execute_alter_table_detach_partition_stmt_in_transaction_with_search_path(
                client_id,
                &finalize_stmt,
                phase2_xid,
                0,
                configured_search_path,
                &mut phase2_effects,
            );
        let result = self.finish_txn(client_id, phase2_xid, result, &phase2_effects, &[], &[]);
        guard.disarm();
        for (rel, _) in phase2_requests {
            self.table_locks.unlock_table(rel, client_id);
        }
        result
    }

    fn wait_for_detach_phase_one_users(
        &self,
        phase1_xid: TransactionId,
        interrupts: Arc<crate::backend::utils::misc::interrupts::InterruptState>,
    ) -> Result<(), ExecError> {
        loop {
            check_for_interrupts(interrupts.as_ref()).map_err(ExecError::Interrupted)?;
            if self.txns.read().oldest_active_xid() > phase1_xid {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn resolve_detach_partition_relations(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        stmt: &AlterTableDetachPartitionStatement,
        configured_search_path: Option<&[String]>,
        expect_pending: Option<bool>,
    ) -> Result<Option<(BoundRelation, BoundRelation, PgInheritsRow)>, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(parent) = lookup_partition_alter_parent(
            &catalog,
            &stmt.parent_table,
            stmt.if_exists,
            detach_unsupported_action(&stmt.mode),
            Some("DETACH PARTITION"),
        )?
        else {
            return Ok(None);
        };
        let child = lookup_partition_alter_child(&catalog, &stmt.partition_table)?;
        if !matches!(child.relkind, 'r' | 'p' | 'f') {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: stmt.partition_table.clone(),
                expected: "table",
            }));
        }
        let inherit =
            validate_detach_inheritance_state(&catalog, &parent, &child, stmt, expect_pending)?;
        Ok(Some((parent, child, inherit)))
    }

    fn mark_detach_partition_pending_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &AlterTableDetachPartitionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let Some((parent, child, _inherit)) = self.resolve_detach_partition_relations(
            client_id,
            xid,
            cid,
            stmt,
            configured_search_path,
            Some(false),
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        if parent.partitioned_table.as_ref().is_some_and(|row| {
            row.partdefid != 0 && catalog.relation_by_oid(row.partdefid).is_some()
        }) {
            return Err(ExecError::DetailedError {
                message: "cannot detach partitions concurrently when a default partition exists"
                    .into(),
                detail: None,
                hint: None,
                sqlstate: "55000",
            });
        }
        ensure_relation_owner(self, client_id, &parent, &stmt.parent_table)?;
        ensure_relation_owner(self, client_id, &child, &stmt.partition_table)?;

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
            .mark_relation_inheritance_detached_mvcc(child.relation_oid, parent.relation_oid, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    fn add_detached_partition_bound_check_in_transaction(
        &self,
        client_id: ClientId,
        parent: &BoundRelation,
        child: &BoundRelation,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some((constraint_name, expr_sql)) =
            detached_partition_bound_check(&catalog, parent, child)?
        else {
            return Ok(cid);
        };
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
            .create_check_constraint_mvcc(
                child.relation_oid,
                constraint_name,
                true,
                true,
                false,
                expr_sql,
                0,
                true,
                0,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(cid.saturating_add(1))
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize_detach_partition_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &AlterTableDetachPartitionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        expect_pending: bool,
    ) -> Result<StatementResult, ExecError> {
        let Some((parent, child, _inherit)) = self.resolve_detach_partition_relations(
            client_id,
            xid,
            cid,
            stmt,
            configured_search_path,
            Some(expect_pending),
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        ensure_relation_owner(self, client_id, &parent, &stmt.parent_table)?;
        ensure_relation_owner(self, client_id, &child, &stmt.partition_table)?;

        let mut next_cid = cid.saturating_add(1);
        self.validate_referenced_partition_foreign_keys_for_detach_in_transaction(
            client_id,
            xid,
            next_cid,
            child.relation_oid,
            configured_search_path,
        )?;
        let catalog =
            self.lazy_catalog_lookup(client_id, Some((xid, next_cid)), configured_search_path);
        let mut detached_referenced_fk_names = std::collections::BTreeSet::new();
        next_cid = self.drop_referenced_partition_foreign_key_constraints_in_transaction(
            client_id,
            xid,
            next_cid,
            child.relation_oid,
            &catalog,
            &mut detached_referenced_fk_names,
            catalog_effects,
        )?;
        next_cid = self.drop_cloned_parent_row_triggers_from_partition_in_transaction(
            client_id,
            xid,
            next_cid,
            parent.relation_oid,
            child.relation_oid,
            configured_search_path,
            catalog_effects,
        )?;
        next_cid = self.detach_partition_child_foreign_key_constraints_in_transaction(
            client_id,
            xid,
            next_cid,
            parent.relation_oid,
            child.relation_oid,
            &catalog,
            &detached_referenced_fk_names,
            configured_search_path,
            catalog_effects,
        )?;
        next_cid = self.detach_partitioned_index_links_from_partition_in_transaction(
            client_id,
            xid,
            next_cid,
            parent.relation_oid,
            child.relation_oid,
            configured_search_path,
            catalog_effects,
        )?;
        if expect_pending {
            next_cid = self.add_detached_partition_bound_check_in_transaction(
                client_id,
                &parent,
                &child,
                xid,
                next_cid,
                configured_search_path,
                catalog_effects,
            )?;
        }
        next_cid = self.clear_detached_partition_identity_columns_in_transaction(
            client_id,
            xid,
            next_cid,
            &parent,
            &child,
            catalog_effects,
        )?;

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: next_cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .drop_partition_inheritance_parent_mvcc(
                child.relation_oid,
                parent.relation_oid,
                Some(expect_pending),
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        next_cid = next_cid.saturating_add(1);

        self.replace_relation_partition_metadata_in_transaction(
            client_id,
            child.relation_oid,
            false,
            None,
            child.partitioned_table.clone(),
            xid,
            next_cid,
            configured_search_path,
            catalog_effects,
        )?;
        next_cid = next_cid.saturating_add(1);
        if parent
            .partitioned_table
            .as_ref()
            .is_some_and(|row| row.partdefid == child.relation_oid)
        {
            self.update_partitioned_table_default_partition_in_transaction(
                client_id,
                parent.relation_oid,
                0,
                xid,
                next_cid,
                configured_search_path,
                catalog_effects,
            )?;
        }
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    fn clear_detached_partition_identity_columns_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        mut cid: CommandId,
        parent: &BoundRelation,
        child: &BoundRelation,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        for parent_column in parent
            .desc
            .columns
            .iter()
            .filter(|column| !column.dropped && column.identity.is_some())
        {
            let Some(child_column) = child.desc.columns.iter().find(|column| {
                !column.dropped && column.name.eq_ignore_ascii_case(&parent_column.name)
            }) else {
                continue;
            };
            if child_column.identity.is_none()
                && child_column.default_sequence_oid != parent_column.default_sequence_oid
            {
                continue;
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
            let effect = self
                .catalog
                .write()
                .alter_table_set_column_identity_mvcc(
                    child.relation_oid,
                    &child_column.name,
                    None,
                    None,
                    None,
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            cid = cid.saturating_add(1);
        }
        Ok(cid)
    }

    #[allow(clippy::too_many_arguments)]
    fn detach_partitioned_index_links_from_partition_in_transaction(
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
        let parent_index_oids = catalog
            .index_relations_for_heap(parent_oid)
            .into_iter()
            .map(|index| index.relation_oid)
            .collect::<BTreeSet<_>>();
        if parent_index_oids.is_empty() {
            return Ok(cid);
        }
        let child_indexes = catalog.index_relations_for_heap(child_oid);
        let child_constraints = catalog.constraint_rows_for_relation(child_oid);
        let mut next_cid = cid;

        for child_index in child_indexes {
            let parent_index_links = catalog
                .inheritance_parents(child_index.relation_oid)
                .into_iter()
                .filter(|row| parent_index_oids.contains(&row.inhparent))
                .map(|row| row.inhparent)
                .collect::<Vec<_>>();
            for parent_index_oid in parent_index_links {
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: next_cid,
                    client_id,
                    waiter: None,
                    interrupts: self.interrupt_state(client_id),
                };
                let effect = self
                    .catalog
                    .write()
                    .drop_partition_inheritance_parent_mvcc(
                        child_index.relation_oid,
                        parent_index_oid,
                        None,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                next_cid = next_cid.saturating_add(1);

                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: next_cid,
                    client_id,
                    waiter: None,
                    interrupts: self.interrupt_state(client_id),
                };
                let effect = self
                    .catalog
                    .write()
                    .replace_relation_partitioning_mvcc(
                        child_index.relation_oid,
                        false,
                        None,
                        None,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                next_cid = next_cid.saturating_add(1);
            }

            for constraint in child_constraints.iter().filter(|row| {
                row.conindid == child_index.relation_oid
                    && row.conparentid != 0
                    && matches!(row.contype, CONSTRAINT_PRIMARY | CONSTRAINT_UNIQUE)
            }) {
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: next_cid,
                    client_id,
                    waiter: None,
                    interrupts: self.interrupt_state(client_id),
                };
                let effect = self
                    .catalog
                    .write()
                    .update_index_backed_constraint_inheritance_mvcc(
                        child_oid,
                        constraint.oid,
                        0,
                        true,
                        0,
                        false,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                next_cid = next_cid.saturating_add(1);
            }
        }
        Ok(next_cid)
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::executor::{ExecError, StatementResult, Value};
    use crate::backend::parser::{
        AlterTableDetachPartitionStatement, DetachPartitionMode, ParseError,
    };
    use crate::pgrust::database::Database;
    use crate::pgrust::session::Session;
    use crate::pl::plpgsql::{clear_notices, take_notices};
    use std::path::PathBuf;

    fn temp_dir(label: &str) -> PathBuf {
        crate::pgrust::test_support::seeded_temp_dir("partition", label)
    }

    fn query_rows(session: &mut Session, db: &Database, sql: &str) -> Vec<Vec<Value>> {
        match session.execute(db, sql).unwrap() {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        }
    }

    fn take_notice_messages() -> Vec<String> {
        take_notices()
            .into_iter()
            .map(|notice| notice.message)
            .collect()
    }

    #[test]
    fn partition_introspection_functions_follow_declarative_tree() {
        let base = temp_dir("introspection");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table measurement (a int4, b int4) partition by range (a)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table measurement_lo partition of measurement \
                 for values from (minvalue) to (10) partition by list (b)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table measurement_lo_list partition of measurement_lo for values in (1)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table measurement_hi partition of measurement \
                 for values from (10) to (20)",
            )
            .unwrap();
        session
            .execute(&db, "create table plain_table (a int4)")
            .unwrap();

        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select relid::regclass::text, coalesce(parentrelid::regclass::text, ''), \
                        level, isleaf \
                   from pg_partition_tree('measurement') \
                  order by level, relid::regclass::text",
            ),
            vec![
                vec![
                    Value::Text("measurement".into()),
                    Value::Text("".into()),
                    Value::Int32(0),
                    Value::Bool(false),
                ],
                vec![
                    Value::Text("measurement_hi".into()),
                    Value::Text("measurement".into()),
                    Value::Int32(1),
                    Value::Bool(true),
                ],
                vec![
                    Value::Text("measurement_lo".into()),
                    Value::Text("measurement".into()),
                    Value::Int32(1),
                    Value::Bool(false),
                ],
                vec![
                    Value::Text("measurement_lo_list".into()),
                    Value::Text("measurement_lo".into()),
                    Value::Int32(2),
                    Value::Bool(true),
                ],
            ]
        );

        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select relid::regclass::text from pg_partition_ancestors('measurement_lo_list')",
            ),
            vec![
                vec![Value::Text("measurement_lo_list".into())],
                vec![Value::Text("measurement_lo".into())],
                vec![Value::Text("measurement".into())],
            ]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select pg_partition_root('measurement_lo_list')::regclass::text",
            ),
            vec![vec![Value::Text("measurement".into())]]
        );
        assert_eq!(
            query_rows(&mut session, &db, "select * from pg_partition_tree(0)"),
            Vec::<Vec<Value>>::new()
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select * from pg_partition_ancestors(null)"
            ),
            Vec::<Vec<Value>>::new()
        );
        assert_eq!(
            query_rows(&mut session, &db, "select pg_partition_root(0)"),
            vec![vec![Value::Null]]
        );
        assert_eq!(
            query_rows(&mut session, &db, "select pg_partition_root('plain_table')",),
            vec![vec![Value::Null]]
        );
    }

    #[test]
    fn partitioned_root_dml_routes_rows_and_only_root_is_empty() {
        let base = temp_dir("routing");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table routed (a int4, b text) partition by list (a)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table routed1 partition of routed for values in (1)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table routed2 partition of routed for values in (2)",
            )
            .unwrap();

        assert_eq!(
            session
                .execute(&db, "insert into routed values (1, 'one'), (2, 'two')")
                .unwrap(),
            StatementResult::AffectedRows(2)
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select tableoid::regclass::text, a, b from routed order by 1, 2",
            ),
            vec![
                vec![
                    Value::Text("routed1".into()),
                    Value::Int32(1),
                    Value::Text("one".into()),
                ],
                vec![
                    Value::Text("routed2".into()),
                    Value::Int32(2),
                    Value::Text("two".into()),
                ],
            ]
        );
        assert_eq!(
            query_rows(&mut session, &db, "select * from only routed"),
            Vec::<Vec<Value>>::new()
        );

        assert_eq!(
            session
                .execute(&db, "update routed set b = 'uno' where a = 1")
                .unwrap(),
            StatementResult::AffectedRows(1)
        );
        assert_eq!(
            session
                .execute(&db, "delete from routed where a = 2")
                .unwrap(),
            StatementResult::AffectedRows(1)
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select tableoid::regclass::text, a, b from routed order by 1, 2",
            ),
            vec![vec![
                Value::Text("routed1".into()),
                Value::Int32(1),
                Value::Text("uno".into()),
            ]]
        );

        match session.execute(&db, "insert into routed values (3, 'three')") {
            Err(ExecError::DetailedError {
                message,
                detail: Some(detail),
                sqlstate,
                ..
            }) => {
                assert_eq!(message, "no partition of relation \"routed\" found for row");
                assert!(detail.contains("(a) = (3)"));
                assert_eq!(sqlstate, "23514");
            }
            other => panic!("expected no-partition error, got {other:?}"),
        }

        match session.execute(&db, "insert into routed1 values (2, 'bad')") {
            Err(ExecError::DetailedError {
                message,
                detail: Some(detail),
                sqlstate,
                ..
            }) => {
                assert_eq!(
                    message,
                    "new row for relation \"routed1\" violates partition constraint"
                );
                assert!(detail.contains("(2, bad)"));
                assert_eq!(sqlstate, "23514");
            }
            other => panic!("expected partition constraint error, got {other:?}"),
        }

        assert_eq!(
            session
                .execute(&db, "update routed set a = 2 where a = 1")
                .unwrap(),
            StatementResult::AffectedRows(1)
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select tableoid::regclass::text, a, b from routed order by 1, 2",
            ),
            vec![vec![
                Value::Text("routed2".into()),
                Value::Int32(2),
                Value::Text("uno".into()),
            ]]
        );
        assert_eq!(
            query_rows(&mut session, &db, "select * from only routed"),
            Vec::<Vec<Value>>::new()
        );
    }

    #[test]
    fn attach_partition_validates_rows_and_updates_metadata() {
        let base = temp_dir("attach");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table parent_attach (a int4) partition by range (a)",
            )
            .unwrap();
        session
            .execute(&db, "create table child_ok (a int4)")
            .unwrap();
        session
            .execute(&db, "insert into child_ok values (0), (5)")
            .unwrap();

        assert_eq!(
            session
                .execute(
                    &db,
                    "alter table parent_attach attach partition child_ok \
                     for values from (0) to (10)",
                )
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select pg_partition_root('child_ok')::regclass::text",
            ),
            vec![vec![Value::Text("parent_attach".into())]]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select relispartition, relpartbound is not null \
                   from pg_class where relname = 'child_ok'",
            ),
            vec![vec![Value::Bool(true), Value::Bool(true)]]
        );

        session
            .execute(&db, "create table child_bad (a int4)")
            .unwrap();
        session
            .execute(&db, "insert into child_bad values (15)")
            .unwrap();
        match session.execute(
            &db,
            "alter table parent_attach attach partition child_bad \
             for values from (0) to (10)",
        ) {
            Err(ExecError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            }) => {
                assert_eq!(
                    message,
                    "partition constraint of relation \"child_bad\" is violated by some row"
                );
                assert_eq!(detail, None);
                assert_eq!(sqlstate, "23514");
            }
            other => panic!("expected attach validation failure, got {other:?}"),
        }

        session
            .execute(&db, "create table child_overlap (a int4)")
            .unwrap();
        match session.execute(
            &db,
            "alter table parent_attach attach partition child_overlap \
             for values from (5) to (15)",
        ) {
            Err(ExecError::DetailedError {
                message, sqlstate, ..
            }) => {
                assert_eq!(
                    message,
                    "partition \"child_overlap\" would overlap partition \"child_ok\""
                );
                assert_eq!(sqlstate, "42P17");
            }
            other => panic!("expected overlap failure, got {other:?}"),
        }
    }

    #[test]
    fn attach_partition_like_child_preserves_collation_before_overlap_check() {
        let base = temp_dir("attach_like_collation");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table list_parted_like (
                    a int not null,
                    b char(2) collate \"C\",
                    constraint check_a check (a > 0)
                 ) partition by list (a)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table part_like (
                    a int not null,
                    b char(2) collate \"C\",
                    constraint check_a check (a > 0)
                 )",
            )
            .unwrap();
        session
            .execute(
                &db,
                "alter table list_parted_like attach partition part_like for values in (1)",
            )
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select attcollation from pg_attribute
                   where attrelid = 'part_like'::regclass and attname = 'b'",
            ),
            vec![vec![Value::Int64(i64::from(
                crate::include::catalog::C_COLLATION_OID
            ))]]
        );
        session
            .execute(
                &db,
                "create table fail_like (like part_like including constraints)",
            )
            .unwrap();

        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select attcollation from pg_attribute
                   where attrelid = 'fail_like'::regclass and attname = 'b'",
            ),
            vec![vec![Value::Int64(i64::from(
                crate::include::catalog::C_COLLATION_OID
            ))]]
        );

        match session.execute(
            &db,
            "alter table list_parted_like attach partition fail_like for values in (1)",
        ) {
            Err(ExecError::DetailedError {
                message, sqlstate, ..
            }) => {
                assert_eq!(
                    message,
                    "partition \"fail_like\" would overlap partition \"part_like\""
                );
                assert_eq!(sqlstate, "42P17");
            }
            other => panic!("expected overlap failure, got {other:?}"),
        }
    }

    #[test]
    fn inherited_partition_check_added_with_not_null_cannot_be_dropped_from_child() {
        let base = temp_dir("partition_check_drop");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table list_parted_check (
                    a int,
                    b char(1)
                 ) partition by list (a)",
            )
            .unwrap();
        session
            .execute(&db, "create table part_check (like list_parted_check)")
            .unwrap();
        session
            .execute(
                &db,
                "alter table list_parted_check attach partition part_check for values in (2)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "alter table list_parted_check alter b set not null,
                 add constraint check_a2 check (a > 0)",
            )
            .unwrap();

        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select conislocal, coninhcount from pg_constraint
                   where conrelid = 'part_check'::regclass and conname = 'check_a2'",
            ),
            vec![vec![Value::Bool(false), Value::Int16(1)]]
        );

        match session.execute(&db, "alter table part_check drop constraint check_a2") {
            Err(ExecError::DetailedError {
                message, sqlstate, ..
            }) => {
                assert_eq!(
                    message,
                    "cannot drop inherited constraint \"check_a2\" of relation \"part_check\""
                );
                assert_eq!(sqlstate, "42P16");
            }
            other => panic!("expected inherited constraint drop failure, got {other:?}"),
        }
    }

    #[test]
    fn attach_partition_from_parent_statement_trigger_reports_active_query() {
        let base = temp_dir("attach_active_trigger");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table tab_part_attach (a int) partition by list (a)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create function func_part_attach() returns trigger language plpgsql as $$
                 begin
                   execute 'create table tab_part_attach_1 (a int)';
                   execute 'alter table tab_part_attach attach partition tab_part_attach_1 for values in (1)';
                   return null;
                 end $$",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create trigger trig_part_attach before insert on tab_part_attach
                 for each statement execute procedure func_part_attach()",
            )
            .unwrap();

        fn detailed_error(err: &ExecError) -> Option<(&str, &str)> {
            match err {
                ExecError::DetailedError {
                    message, sqlstate, ..
                } => Some((message.as_str(), *sqlstate)),
                ExecError::WithContext { source, .. } => detailed_error(source),
                _ => None,
            }
        }

        match session.execute(&db, "insert into tab_part_attach values (1)") {
            Err(err) if detailed_error(&err).is_some() => {
                let (message, sqlstate) = detailed_error(&err).unwrap();
                assert_eq!(
                    message,
                    "cannot ALTER TABLE \"tab_part_attach\" because it is being used by active queries in this session"
                );
                assert_eq!(sqlstate, "55006");
            }
            other => panic!("expected active-query attach failure, got {other:?}"),
        }
    }

    #[test]
    fn partitioned_update_parent_transition_table_sees_attached_default_rows() {
        let base = temp_dir("partition_update_transition");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table upd_bar1 (a integer, b integer not null default 1)
                   partition by range (a)",
            )
            .unwrap();
        session
            .execute(&db, "create table upd_bar2 (a integer)")
            .unwrap();
        session
            .execute(&db, "insert into upd_bar2 values (1)")
            .unwrap();
        session
            .execute(
                &db,
                "alter table upd_bar2 add column b integer not null default 1",
            )
            .unwrap();
        session
            .execute(
                &db,
                "alter table upd_bar1 attach partition upd_bar2 default",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create function upd_xtrig() returns trigger language plpgsql as $$
                 declare r record;
                 begin
                   for r in select * from old loop
                     raise info 'a=%, b=%', r.a, r.b;
                   end loop;
                   return null;
                 end $$",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create trigger upd_xtrig
                   after update on upd_bar1
                   referencing old table as old
                   for each statement execute procedure upd_xtrig()",
            )
            .unwrap();

        clear_notices();
        session
            .execute(&db, "update upd_bar1 set a = a + 1")
            .unwrap();
        assert_eq!(take_notice_messages(), vec!["a=1, b=1".to_string()]);
    }

    #[test]
    fn attach_partition_rejects_circular_inheritance_before_catalog_write() {
        let base = temp_dir("attach_cycle");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table list_parted2 (a int4, b text) partition by list (a)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table part_5 (like list_parted2) partition by list (b)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "alter table list_parted2 attach partition part_5 for values in (5)",
            )
            .unwrap();

        match session.execute(
            &db,
            "alter table part_5 attach partition list_parted2 for values in ('b')",
        ) {
            Err(ExecError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            }) => {
                assert_eq!(message, "circular inheritance not allowed");
                assert_eq!(
                    detail.as_deref(),
                    Some("\"part_5\" is already a child of \"list_parted2\".")
                );
                assert_eq!(sqlstate, "42P17");
            }
            other => panic!("expected circular attach partition error, got {other:?}"),
        }
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select count(*) from pg_inherits \
                 where inhrelid = 'list_parted2'::regclass \
                   and inhparent = 'part_5'::regclass",
            ),
            vec![vec![Value::Int64(0)]]
        );

        match session.execute(
            &db,
            "alter table list_parted2 attach partition list_parted2 for values in (0)",
        ) {
            Err(ExecError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            }) => {
                assert_eq!(message, "circular inheritance not allowed");
                assert_eq!(
                    detail.as_deref(),
                    Some("\"list_parted2\" is already a child of \"list_parted2\".")
                );
                assert_eq!(sqlstate, "42P17");
            }
            other => panic!("expected self attach partition error, got {other:?}"),
        }
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select count(*) from pg_inherits \
                 where inhrelid = 'list_parted2'::regclass \
                   and inhparent = 'list_parted2'::regclass",
            ),
            vec![vec![Value::Int64(0)]]
        );
    }

    #[test]
    fn attach_partition_rejects_rows_already_in_default_partition() {
        let base = temp_dir("attach_default_rows");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table attach_default_parent (a int4) partition by list (a)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table attach_default_fallback partition of attach_default_parent default",
            )
            .unwrap();
        session
            .execute(&db, "insert into attach_default_parent values (1), (2)")
            .unwrap();
        session
            .execute(&db, "create table attach_default_one (a int4)")
            .unwrap();

        match session.execute(
            &db,
            "alter table attach_default_parent attach partition attach_default_one \
             for values in (1)",
        ) {
            Err(ExecError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            }) => {
                assert_eq!(
                    message,
                    "updated partition constraint for default partition \"attach_default_fallback\" would be violated by some row"
                );
                assert_eq!(detail, None);
                assert_eq!(sqlstate, "23514");
            }
            other => panic!("expected default partition validation failure, got {other:?}"),
        }

        session
            .execute(&db, "delete from attach_default_fallback where a = 1")
            .unwrap();
        assert_eq!(
            session
                .execute(
                    &db,
                    "alter table attach_default_parent attach partition attach_default_one \
                     for values in (1)",
                )
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
        session
            .execute(&db, "insert into attach_default_parent values (1)")
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select tableoid::regclass::text, a from attach_default_parent order by a",
            ),
            vec![
                vec![Value::Text("attach_default_one".into()), Value::Int32(1),],
                vec![
                    Value::Text("attach_default_fallback".into()),
                    Value::Int32(2),
                ],
            ]
        );
    }

    #[test]
    fn detach_partition_updates_metadata_and_routing() {
        let base = temp_dir("detach_immediate");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table detach_root (a int4, b text) partition by list (a)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table detach_one partition of detach_root for values in (1)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table detach_two partition of detach_root for values in (2)",
            )
            .unwrap();
        session
            .execute(&db, "insert into detach_root values (1, 'one'), (2, 'two')")
            .unwrap();

        assert_eq!(
            session
                .execute(&db, "alter table detach_root detach partition detach_one")
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select relispartition, relpartbound is null \
                   from pg_class where relname = 'detach_one'",
            ),
            vec![vec![Value::Bool(false), Value::Bool(true)]]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select inhrelid from pg_inherits \
                  where inhrelid = 'detach_one'::regclass \
                    and inhparent = 'detach_root'::regclass",
            ),
            Vec::<Vec<Value>>::new()
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select tableoid::regclass::text, a, b from detach_root order by a",
            ),
            vec![vec![
                Value::Text("detach_two".into()),
                Value::Int32(2),
                Value::Text("two".into()),
            ]]
        );
        assert_eq!(
            query_rows(&mut session, &db, "select a, b from detach_one"),
            vec![vec![Value::Int32(1), Value::Text("one".into())]]
        );
        session
            .execute(&db, "insert into detach_one values (9, 'direct')")
            .unwrap();
        match session.execute(&db, "insert into detach_root values (1, 'again')") {
            Err(ExecError::DetailedError {
                message, sqlstate, ..
            }) => {
                assert_eq!(
                    message,
                    "no partition of relation \"detach_root\" found for row"
                );
                assert_eq!(sqlstate, "23514");
            }
            other => panic!("expected no-partition error, got {other:?}"),
        }
    }

    #[test]
    fn detach_default_partition_clears_default_oid() {
        let base = temp_dir("detach_default");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table detach_default_root (a int4) partition by list (a)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table detach_default_fallback partition of detach_default_root default",
            )
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select partdefid = 'detach_default_fallback'::regclass \
                   from pg_partitioned_table \
                  where partrelid = 'detach_default_root'::regclass",
            ),
            vec![vec![Value::Bool(true)]]
        );
        session
            .execute(
                &db,
                "alter table detach_default_root detach partition detach_default_fallback",
            )
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select partdefid = 0 from pg_partitioned_table \
                  where partrelid = 'detach_default_root'::regclass",
            ),
            vec![vec![Value::Bool(true)]]
        );
        match session.execute(&db, "insert into detach_default_root values (99)") {
            Err(ExecError::DetailedError {
                message, sqlstate, ..
            }) => {
                assert_eq!(
                    message,
                    "no partition of relation \"detach_default_root\" found for row"
                );
                assert_eq!(sqlstate, "23514");
            }
            other => panic!("expected no-partition error, got {other:?}"),
        }
    }

    #[test]
    fn detach_subpartitioned_child_preserves_its_partition_tree() {
        let base = temp_dir("detach_subpartitioned");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table detach_region \
                 (a int4, b int4, constraint detach_region_a_pos check (a > 0)) \
                 partition by list (a)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table detach_region_one partition of detach_region \
                 for values in (1) partition by range (b)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table detach_region_one_lo partition of detach_region_one \
                 for values from (0) to (10)",
            )
            .unwrap();

        session
            .execute(
                &db,
                "alter table detach_region detach partition detach_region_one",
            )
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select relispartition from pg_class where relname = 'detach_region_one'",
            ),
            vec![vec![Value::Bool(false)]]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select pg_partition_root('detach_region_one_lo')::regclass::text",
            ),
            vec![vec![Value::Text("detach_region_one".into())]]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select relid::regclass::text from pg_partition_tree('detach_region')",
            ),
            vec![vec![Value::Text("detach_region".into())]]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select relid::regclass::text, coalesce(parentrelid::regclass::text, '') \
                   from pg_partition_tree('detach_region_one') \
                  order by level, relid::regclass::text",
            ),
            vec![
                vec![
                    Value::Text("detach_region_one".into()),
                    Value::Text("".into()),
                ],
                vec![
                    Value::Text("detach_region_one_lo".into()),
                    Value::Text("detach_region_one".into()),
                ],
            ]
        );
        session
            .execute(
                &db,
                "alter table detach_region_one drop constraint detach_region_a_pos",
            )
            .unwrap();
        session
            .execute(&db, "insert into detach_region_one values (-1, 5)")
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select tableoid::regclass::text, a, b from detach_region_one",
            ),
            vec![vec![
                Value::Text("detach_region_one_lo".into()),
                Value::Int32(-1),
                Value::Int32(5),
            ]]
        );
    }

    #[test]
    fn detach_partition_detaches_indexes_constraints_and_trigger_clones() {
        let base = temp_dir("detach_cleanup");
        let db = Database::open(&base, 32).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table detach_cleanup_parent (a int4 primary key, b int4) \
                 partition by range (a)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table detach_cleanup_p1 partition of detach_cleanup_parent \
                 for values from (0) to (10)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create function detach_cleanup_trigger() returns trigger language plpgsql as $$ \
                 begin return new; end $$",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create trigger detach_cleanup_ai after insert on detach_cleanup_parent \
                 for each row execute function detach_cleanup_trigger()",
            )
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select child.relname, parent.relname \
                   from pg_inherits i \
                   join pg_class child on child.oid = i.inhrelid \
                   join pg_class parent on parent.oid = i.inhparent \
                  where parent.relname = 'detach_cleanup_parent_pkey'",
            ),
            vec![vec![
                Value::Text("detach_cleanup_p1_pkey".into()),
                Value::Text("detach_cleanup_parent_pkey".into()),
            ]]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select conparentid <> 0 from pg_constraint \
                  where conrelid = 'detach_cleanup_p1'::regclass \
                    and contype::text = 'p'",
            ),
            vec![vec![Value::Bool(true)]]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select c.relname, t.tgparentid <> 0 \
                   from pg_trigger t join pg_class c on c.oid = t.tgrelid \
                  where t.tgname = 'detach_cleanup_ai' \
                  order by c.relname",
            ),
            vec![
                vec![Value::Text("detach_cleanup_p1".into()), Value::Bool(true)],
                vec![
                    Value::Text("detach_cleanup_parent".into()),
                    Value::Bool(false),
                ],
            ]
        );

        session
            .execute(
                &db,
                "alter table detach_cleanup_parent detach partition detach_cleanup_p1",
            )
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select child.relname, parent.relname \
                   from pg_inherits i \
                   join pg_class child on child.oid = i.inhrelid \
                   join pg_class parent on parent.oid = i.inhparent \
                  where parent.relname = 'detach_cleanup_parent_pkey'",
            ),
            Vec::<Vec<Value>>::new()
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select relispartition \
                   from pg_class \
                  where relname = 'detach_cleanup_p1_pkey'",
            ),
            vec![vec![Value::Bool(false)]]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select deptype::text \
                   from pg_depend \
                  where objid = 'detach_cleanup_p1_pkey'::regclass \
                    and deptype::text in ('P', 'S') \
                  order by deptype::text",
            ),
            Vec::<Vec<Value>>::new()
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select conparentid = 0, conislocal, coninhcount \
                   from pg_constraint \
                  where conrelid = 'detach_cleanup_p1'::regclass \
                    and contype::text = 'p'",
            ),
            vec![vec![Value::Bool(true), Value::Bool(true), Value::Int16(0)]]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select c.relname, t.tgparentid <> 0 \
                   from pg_trigger t join pg_class c on c.oid = t.tgrelid \
                  where t.tgname = 'detach_cleanup_ai' \
                  order by c.relname",
            ),
            vec![vec![
                Value::Text("detach_cleanup_parent".into()),
                Value::Bool(false),
            ]]
        );
    }

    #[test]
    fn detach_partition_concurrently_rejects_transaction_and_default_partition() {
        let base = temp_dir("detach_concurrently_rejects");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table detach_conc_parent (a int4) partition by range (a)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table detach_conc_p1 partition of detach_conc_parent \
                 for values from (0) to (10)",
            )
            .unwrap();
        session.execute(&db, "begin").unwrap();
        match session.execute(
            &db,
            "alter table detach_conc_parent detach partition detach_conc_p1 concurrently",
        ) {
            Err(ExecError::Parse(ParseError::ActiveSqlTransaction(stmt))) => {
                assert_eq!(stmt, "ALTER TABLE ... DETACH CONCURRENTLY");
            }
            other => panic!("expected active transaction error, got {other:?}"),
        }
        session.execute(&db, "rollback").unwrap();

        session
            .execute(
                &db,
                "create table detach_conc_default (a int4) partition by list (a)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table detach_conc_default_p1 partition of detach_conc_default \
                 for values in (1)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table detach_conc_default_fallback \
                 partition of detach_conc_default default",
            )
            .unwrap();
        match session.execute(
            &db,
            "alter table detach_conc_default detach partition detach_conc_default_p1 concurrently",
        ) {
            Err(ExecError::DetailedError {
                message, sqlstate, ..
            }) => {
                assert_eq!(
                    message,
                    "cannot detach partitions concurrently when a default partition exists"
                );
                assert_eq!(sqlstate, "55000");
            }
            other => panic!("expected default partition rejection, got {other:?}"),
        }
    }

    #[test]
    fn detach_partition_concurrently_reaches_final_state() {
        let base = temp_dir("detach_concurrently");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table detach_conc_ok (a int4) partition by range (a)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table detach_conc_ok_p1 partition of detach_conc_ok \
                 for values from (0) to (10)",
            )
            .unwrap();
        assert_eq!(
            session
                .execute(
                    &db,
                    "alter table detach_conc_ok detach partition detach_conc_ok_p1 concurrently",
                )
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select relispartition, relpartbound is null \
                   from pg_class where relname = 'detach_conc_ok_p1'",
            ),
            vec![vec![Value::Bool(false), Value::Bool(true)]]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select * from pg_inherits \
                  where inhrelid = 'detach_conc_ok_p1'::regclass \
                    and inhparent = 'detach_conc_ok'::regclass",
            ),
            Vec::<Vec<Value>>::new()
        );
    }

    #[test]
    fn detach_partition_finalize_completes_pending_catalog_row() {
        let base = temp_dir("detach_finalize");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table detach_finalize_parent (a int4) partition by range (a)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table detach_finalize_p1 partition of detach_finalize_parent \
                 for values from (0) to (10)",
            )
            .unwrap();
        let stmt = AlterTableDetachPartitionStatement {
            if_exists: false,
            only: false,
            parent_table: "detach_finalize_parent".into(),
            partition_table: "detach_finalize_p1".into(),
            mode: DetachPartitionMode::Concurrently,
        };
        let xid = db.txns.write().begin();
        let mut catalog_effects = Vec::new();
        let result = db.mark_detach_partition_pending_in_transaction(
            1,
            &stmt,
            xid,
            0,
            None,
            &mut catalog_effects,
        );
        db.finish_txn(1, xid, result, &catalog_effects, &[], &[])
            .unwrap();

        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select inhdetachpending from pg_inherits \
                  where inhrelid = 'detach_finalize_p1'::regclass \
                    and inhparent = 'detach_finalize_parent'::regclass",
            ),
            vec![vec![Value::Bool(true)]]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select relid::regclass::text from pg_partition_tree('detach_finalize_parent')",
            ),
            vec![vec![Value::Text("detach_finalize_parent".into())]]
        );
        assert_eq!(
            session
                .execute(
                    &db,
                    "alter table detach_finalize_parent detach partition detach_finalize_p1 finalize",
                )
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select relispartition from pg_class where relname = 'detach_finalize_p1'",
            ),
            vec![vec![Value::Bool(false)]]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select * from pg_inherits \
                  where inhrelid = 'detach_finalize_p1'::regclass",
            ),
            Vec::<Vec<Value>>::new()
        );
    }
}
