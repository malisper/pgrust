use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use super::super::*;
use crate::backend::commands::partition::{
    validate_default_partition_rows_for_new_bound, validate_new_partition_bound,
    validate_partition_relation_compatibility, validate_relation_rows_for_partition_bound,
};
use crate::backend::executor::ExecutorContext;
use crate::backend::parser::{
    AlterTableAttachPartitionStatement, AlterTableDetachPartitionStatement, BoundRelation,
    CatalogLookup, DetachPartitionMode, lower_partition_bound_for_relation,
    serialize_partition_bound,
};
use crate::backend::storage::lmgr::{TableLockMode, lock_table_requests_interruptible};
use crate::backend::utils::misc::interrupts::check_for_interrupts;
use crate::include::catalog::{CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE, PgInheritsRow};

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

fn lookup_partition_alter_parent(
    catalog: &dyn CatalogLookup,
    table_name: &str,
    if_exists: bool,
) -> Result<Option<BoundRelation>, ExecError> {
    let Some(parent) = catalog.lookup_any_relation(table_name) else {
        if if_exists {
            return Ok(None);
        }
        return Err(ExecError::Parse(ParseError::TableDoesNotExist(
            table_name.to_string(),
        )));
    };
    if parent.relkind != 'p' || parent.partitioned_table.is_none() {
        return Err(ExecError::Parse(ParseError::WrongObjectType {
            name: table_name.to_string(),
            expected: "partitioned table",
        }));
    }
    Ok(Some(parent))
}

fn lookup_partition_alter_child(
    catalog: &dyn CatalogLookup,
    table_name: &str,
) -> Result<BoundRelation, ExecError> {
    catalog
        .lookup_any_relation(table_name)
        .ok_or_else(|| ExecError::Parse(ParseError::TableDoesNotExist(table_name.to_string())))
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
        sequences: Some(db.sequences.clone()),
        large_objects: Some(db.large_objects.clone()),
        async_notify_runtime: Some(db.async_notify_runtime.clone()),
        advisory_locks: std::sync::Arc::clone(&db.advisory_locks),
        row_locks: std::sync::Arc::clone(&db.row_locks),
        checkpoint_stats: db.checkpoint_stats_snapshot(),
        datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
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
        expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
        case_test_values: Vec::new(),
        system_bindings: Vec::new(),
        subplans: Vec::new(),
        timed: false,
        allow_side_effects: false,
        pending_async_notifications: Vec::new(),
        catalog: catalog.materialize_visible_catalog(),
        compiled_functions: std::collections::HashMap::new(),
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
        let Some(parent) =
            lookup_partition_alter_parent(&catalog, &stmt.parent_table, stmt.if_exists)?
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
        let Some(parent) =
            lookup_partition_alter_parent(&catalog, &stmt.parent_table, stmt.if_exists)?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let child = lookup_partition_alter_child(&catalog, &stmt.partition_table)?;
        ensure_relation_owner(self, client_id, &parent, &stmt.parent_table)?;
        ensure_relation_owner(self, client_id, &child, &stmt.partition_table)?;
        validate_partition_relation_compatibility(
            &catalog,
            &parent,
            &stmt.parent_table,
            &child,
            &stmt.partition_table,
        )?;

        let bound = lower_partition_bound_for_relation(&parent, &stmt.bound, &catalog)
            .map_err(ExecError::Parse)?;
        let mut ctx = ddl_executor_context(
            self,
            &catalog,
            client_id,
            xid,
            cid.saturating_add(1),
            std::sync::Arc::clone(&interrupts),
        )?;
        validate_relation_rows_for_partition_bound(&catalog, &parent, &child, &bound, &mut ctx)?;
        validate_default_partition_rows_for_new_bound(&catalog, &parent, &bound, &mut ctx)?;
        validate_new_partition_bound(
            &catalog,
            &parent,
            &stmt.partition_table,
            &bound,
            Some(child.relation_oid),
        )?;

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

        let relpartbound = Some(serialize_partition_bound(&bound).map_err(ExecError::Parse)?);
        let updated_child = self.replace_relation_partition_metadata_in_transaction(
            client_id,
            child.relation_oid,
            true,
            relpartbound,
            child.partitioned_table.clone(),
            xid,
            cid.saturating_add(3),
            configured_search_path,
            catalog_effects,
        )?;
        if bound.is_default() {
            self.update_partitioned_table_default_partition_in_transaction(
                client_id,
                parent.relation_oid,
                updated_child.relation_oid,
                xid,
                cid.saturating_add(4),
                configured_search_path,
                catalog_effects,
            )?;
        }
        let next_cid = self.reconcile_partitioned_parent_keys_for_attached_child_in_transaction(
            client_id,
            xid,
            cid.saturating_add(5),
            parent.relation_oid,
            updated_child.relation_oid,
            configured_search_path,
            catalog_effects,
        )?;
        let next_cid = self
            .reconcile_partitioned_parent_indexes_for_attached_child_in_transaction(
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
        let Some(parent) =
            lookup_partition_alter_parent(&catalog, &stmt.parent_table, stmt.if_exists)?
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
                ParseError::ActiveSqlTransaction("ALTER TABLE ... DETACH PARTITION CONCURRENTLY"),
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
        let Some(parent) =
            lookup_partition_alter_parent(&catalog, &stmt.parent_table, stmt.if_exists)?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let child = lookup_partition_alter_child(&catalog, &stmt.partition_table)?;
        validate_detach_inheritance_state(&catalog, &parent, &child, stmt, Some(false))?;
        if parent
            .partitioned_table
            .as_ref()
            .is_some_and(|row| row.partdefid != 0)
        {
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
        let Some(parent) =
            lookup_partition_alter_parent(&catalog, &stmt.parent_table, stmt.if_exists)?
        else {
            return Ok(None);
        };
        let child = lookup_partition_alter_child(&catalog, &stmt.partition_table)?;
        if !matches!(child.relkind, 'r' | 'p') {
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
        if parent
            .partitioned_table
            .as_ref()
            .is_some_and(|row| row.partdefid != 0)
        {
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
        next_cid = self.drop_cloned_parent_row_triggers_from_partition_in_transaction(
            client_id,
            xid,
            next_cid,
            parent.relation_oid,
            child.relation_oid,
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
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };

        for child_index in child_indexes {
            let parent_index_links = catalog
                .inheritance_parents(child_index.relation_oid)
                .into_iter()
                .filter(|row| parent_index_oids.contains(&row.inhparent))
                .map(|row| row.inhparent)
                .collect::<Vec<_>>();
            for parent_index_oid in parent_index_links {
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
            }

            for constraint in child_constraints.iter().filter(|row| {
                row.conindid == child_index.relation_oid
                    && row.conparentid != 0
                    && matches!(row.contype, CONSTRAINT_PRIMARY | CONSTRAINT_UNIQUE)
            }) {
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
            }
        }
        Ok(cid.saturating_add(1))
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

        match session.execute(&db, "update routed set a = 2 where a = 1") {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(message))) => {
                assert_eq!(
                    message,
                    "updating partition key columns on partitioned tables"
                );
            }
            other => panic!("expected partition-key update rejection, got {other:?}"),
        }
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
                detail: Some(detail),
                sqlstate,
                ..
            }) => {
                assert_eq!(
                    message,
                    "new row for relation \"child_bad\" violates partition constraint"
                );
                assert!(detail.contains("(15)"));
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
                detail: Some(detail),
                sqlstate,
                ..
            }) => {
                assert_eq!(
                    message,
                    "new row for relation \"attach_default_fallback\" violates partition constraint"
                );
                assert!(detail.contains("(1)"));
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
                "create table detach_region (a int4, b int4) partition by list (a)",
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
                assert_eq!(stmt, "ALTER TABLE ... DETACH PARTITION CONCURRENTLY");
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
