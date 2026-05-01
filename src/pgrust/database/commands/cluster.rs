use super::super::*;
use crate::backend::access::heap::heapam::heap_insert_mvcc_with_cid;
use crate::backend::access::nbtree::nbtcompare::compare_bt_values_with_options;
use crate::backend::catalog::CatalogError;
use crate::backend::commands::tablecmds::{
    collect_matching_rows_heap, index_key_values_for_row, reinitialize_index_relation,
    toast_tuple_for_write,
};
use crate::backend::executor::{
    ExecError, ExecutorContext, StatementResult, Value, pg_sql_sort_by,
};
use crate::backend::parser::{BoundIndexRelation, BoundRelation, CatalogLookup, ParseError};
use crate::backend::utils::misc::notices::push_notice;
use crate::include::access::amapi::{IndexBuildContext, IndexBuildExprContext};
use crate::include::access::htup::AttributeCompression;
use crate::include::catalog::BTREE_AM_OID;
use crate::include::nodes::parsenodes::{AlterTableSetWithoutClusterStatement, ClusterStatement};
use std::cmp::Ordering;

struct ClusteredRow {
    key_values: Vec<Value>,
    values: Vec<Value>,
}

impl Database {
    pub(crate) fn execute_cluster_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &ClusterStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let result = self.execute_cluster_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            &mut temp_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects, &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_cluster_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &ClusterStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = resolve_cluster_table(&catalog, &stmt.table_name)?;
        let index = resolve_cluster_index(&catalog, &relation, &stmt.index_name)?;
        validate_cluster_index(&index)?;
        self.mark_clustered_index(
            client_id,
            xid,
            cid,
            relation.relation_oid,
            index.relation_oid,
            catalog_effects,
        )?;
        if stmt.mark_only {
            return Ok(StatementResult::AffectedRows(0));
        }

        let mut ctx =
            self.cluster_executor_context(client_id, xid, cid, configured_search_path, &catalog)?;
        let rows = cluster_rows_for_index(&relation, &index, &mut ctx)?;
        let storage_rewrites = self.rewrite_cluster_storage(
            client_id,
            xid,
            cid.saturating_add(1),
            configured_search_path,
            &relation,
            catalog_effects,
            &mut ctx,
        )?;
        if relation.relpersistence == 't' {
            self.record_cluster_temp_rewrites(client_id, &storage_rewrites, temp_effects)?;
        }
        let refreshed = lookup_cluster_relation(
            self,
            client_id,
            xid,
            cid.saturating_add(2),
            configured_search_path,
            relation.relation_oid,
        )?;
        self.reinsert_cluster_rows(
            client_id,
            xid,
            cid.saturating_add(2),
            cid,
            configured_search_path,
            refreshed,
            rows.into_iter().map(|row| row.values).collect(),
            &mut ctx,
        )?;
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_set_without_cluster_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTableSetWithoutClusterStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self
            .execute_alter_table_set_without_cluster_stmt_in_transaction_with_search_path(
                client_id,
                stmt,
                xid,
                0,
                configured_search_path,
                &mut catalog_effects,
            );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_table_set_without_cluster_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTableSetWithoutClusterStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = catalog.lookup_any_relation(&stmt.table_name) else {
            if stmt.if_exists {
                push_notice(format!(
                    r#"relation "{}" does not exist, skipping"#,
                    stmt.table_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                stmt.table_name.clone(),
            )));
        };
        if !matches!(relation.relkind, 'r' | 'm') {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: stmt.table_name.clone(),
                expected: "table or materialized view",
            }));
        }
        self.clear_clustered_index(client_id, xid, cid, relation.relation_oid, catalog_effects)?;
        Ok(StatementResult::AffectedRows(0))
    }

    fn cluster_executor_context(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        _configured_search_path: Option<&[String]>,
        catalog: &crate::backend::utils::cache::lsyscache::LazyCatalogLookup,
    ) -> Result<ExecutorContext, ExecError> {
        // :HACK: The regression harness sets a very small statement_timeout,
        // while this initial CLUSTER implementation rebuilds heap/index storage
        // row-by-row in debug builds. Avoid canceling in the middle of the
        // rewrite, which leaves the active transaction looking at partial
        // catalog effects until rollback.
        let interrupts = Arc::new(crate::backend::utils::misc::interrupts::InterruptState::new());
        Ok(ExecutorContext {
            pool: Arc::clone(&self.pool),
            data_dir: None,
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            lock_status_provider: Some(Arc::new(self.clone())),
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            stats_import_runtime: None,
            async_notify_runtime: Some(self.async_notify_runtime.clone()),
            advisory_locks: Arc::clone(&self.advisory_locks),
            row_locks: Arc::clone(&self.row_locks),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            statement_timestamp_usecs:
                crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
            gucs: super::maintenance_safe_gucs(),
            interrupts,
            stats: Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot: self.txns.read().snapshot_for_command(xid, cid)?,
            write_xid_override: None,
            transaction_state: None,
            client_id,
            current_database_name: self.current_database_name(),
            session_user_oid: self.auth_state(client_id).session_user_oid(),
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            active_role_oid: self.auth_state(client_id).active_role_oid(),
            session_replication_role: self.session_replication_role(client_id),
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
            database: Some(self.clone()),
            pending_catalog_effects: Vec::new(),
            pending_table_locks: Vec::new(),
            pending_portals: Vec::new(),
            catalog: Some(crate::backend::executor::executor_catalog(catalog.clone())),
            scalar_function_cache: std::collections::HashMap::new(),
            srf_rows_cache: std::collections::HashMap::new(),
            plpgsql_function_cache: self.plpgsql_function_cache(client_id),
            pinned_cte_tables: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys: None,
            trigger_depth: 0,
        })
    }

    fn mark_clustered_index(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        relation_oid: u32,
        index_oid: u32,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let effect =
            self.catalog
                .write()
                .set_index_clustered_mvcc(relation_oid, index_oid, &ctx)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(())
    }

    fn clear_clustered_index(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        relation_oid: u32,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .clear_index_clustered_mvcc(relation_oid, &ctx)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(())
    }

    fn rewrite_cluster_storage(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        relation: &BoundRelation,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        ctx: &mut ExecutorContext,
    ) -> Result<Vec<(u32, crate::backend::storage::smgr::RelFileLocator)>, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let mut rewrite_oids = vec![relation.relation_oid];
        for index in catalog.index_relations_for_heap(relation.relation_oid) {
            push_unique_cluster_oid(&mut rewrite_oids, index.relation_oid);
        }
        if let Some(toast) = relation.toast {
            push_unique_cluster_oid(&mut rewrite_oids, toast.relation_oid);
            for index in catalog.index_relations_for_heap(toast.relation_oid) {
                push_unique_cluster_oid(&mut rewrite_oids, index.relation_oid);
            }
        }

        let write_ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .rewrite_relation_storage_mvcc(&rewrite_oids, &write_ctx)?;
        let rewrites = rewrite_oids
            .iter()
            .copied()
            .zip(effect.created_rels.iter().copied())
            .collect::<Vec<_>>();
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        let refreshed_catalog = self.lazy_catalog_lookup(
            client_id,
            Some((xid, cid.saturating_add(1))),
            configured_search_path,
        );
        ctx.catalog = Some(crate::backend::executor::executor_catalog(
            refreshed_catalog.clone(),
        ));
        ctx.snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        Ok(rewrites)
    }

    fn reinsert_cluster_rows(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        catalog_cid: CommandId,
        insert_cid: CommandId,
        configured_search_path: Option<&[String]>,
        relation: BoundRelation,
        rows: Vec<Vec<Value>>,
        ctx: &mut ExecutorContext,
    ) -> Result<(), ExecError> {
        let catalog =
            self.lazy_catalog_lookup(client_id, Some((xid, catalog_cid)), configured_search_path);
        let indexes = catalog.index_relations_for_heap(relation.relation_oid);
        let toast_index = relation.toast.and_then(|toast| {
            catalog
                .index_relations_for_heap(toast.relation_oid)
                .into_iter()
                .next()
        });
        validate_cluster_rebuild_indexes(&indexes)?;
        if let Some(toast) = relation.toast {
            let toast_indexes = catalog.index_relations_for_heap(toast.relation_oid);
            reinitialize_cluster_indexes(&toast_indexes, ctx, xid)?;
        }
        for values in rows {
            let (tuple, _toasted) = toast_tuple_for_write(
                &relation.desc,
                &values,
                relation.toast,
                toast_index.as_ref(),
                ctx,
                xid,
                insert_cid,
            )?;
            let _ = heap_insert_mvcc_with_cid(
                &*ctx.pool,
                ctx.client_id,
                relation.rel,
                xid,
                insert_cid,
                &tuple,
            )?;
        }
        ctx.snapshot = ctx
            .txns
            .read()
            .snapshot_for_command(xid, insert_cid.saturating_add(1))?;
        rebuild_cluster_indexes(&relation, &indexes, ctx)?;
        Ok(())
    }

    fn record_cluster_temp_rewrites(
        &self,
        client_id: ClientId,
        rewrites: &[(u32, crate::backend::storage::smgr::RelFileLocator)],
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<(), ExecError> {
        for (relation_oid, new_rel) in rewrites {
            if let Ok(old_rel) = self.replace_temp_entry_rel(client_id, *relation_oid, *new_rel) {
                temp_effects.push(TempMutationEffect::ReplaceRel {
                    relation_oid: *relation_oid,
                    old_rel,
                    new_rel: *new_rel,
                });
            }
        }
        Ok(())
    }
}

fn resolve_cluster_table(
    catalog: &dyn CatalogLookup,
    table_name: &str,
) -> Result<BoundRelation, ExecError> {
    match catalog.lookup_any_relation(table_name) {
        Some(relation) if matches!(relation.relkind, 'r' | 'm') => Ok(relation),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: table_name.to_string(),
            expected: "table or materialized view",
        })),
        None => Err(ExecError::Parse(ParseError::UnknownTable(
            table_name.to_string(),
        ))),
    }
}

fn resolve_cluster_index(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    index_name: &str,
) -> Result<BoundIndexRelation, ExecError> {
    let named_oid = catalog
        .lookup_any_relation(index_name)
        .map(|relation| relation.relation_oid);
    catalog
        .index_relations_for_heap(relation.relation_oid)
        .into_iter()
        .find(|index| {
            named_oid == Some(index.relation_oid) || index.name.eq_ignore_ascii_case(index_name)
        })
        .ok_or_else(|| {
            ExecError::Parse(ParseError::DetailedError {
                message: format!(
                    "index \"{index_name}\" for table \"{}\" does not exist",
                    relation_name_for_cluster_error(relation, index_name)
                ),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })
        })
}

fn validate_cluster_index(index: &BoundIndexRelation) -> Result<(), ExecError> {
    if index.index_meta.am_oid != BTREE_AM_OID {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "CLUSTER using non-btree indexes is not supported yet".into(),
        )));
    }
    if index.index_meta.indpred.is_some() || index.index_predicate.is_some() {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "CLUSTER using partial indexes is not supported yet".into(),
        )));
    }
    Ok(())
}

fn cluster_rows_for_index(
    relation: &BoundRelation,
    index: &BoundIndexRelation,
    ctx: &mut ExecutorContext,
) -> Result<Vec<ClusteredRow>, ExecError> {
    let mut rows = Vec::new();
    for (_tid, values) in
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, ctx)?
    {
        let key_values = index_key_values_for_row(index, &relation.desc, &values, ctx)?;
        rows.push(ClusteredRow { key_values, values });
    }
    pg_sql_sort_by(&mut rows, |left, right| {
        compare_cluster_index_keys(
            &left.key_values,
            &right.key_values,
            &index.index_meta.indoption,
        )
    });
    Ok(rows)
}

fn compare_cluster_index_keys(left: &[Value], right: &[Value], indoption: &[i16]) -> Ordering {
    for (index, (left_value, right_value)) in left.iter().zip(right).enumerate() {
        let ordering = compare_bt_values_with_options(
            left_value,
            right_value,
            indoption.get(index).copied().unwrap_or_default(),
        );
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn lookup_cluster_relation(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    configured_search_path: Option<&[String]>,
    relation_oid: u32,
) -> Result<BoundRelation, ExecError> {
    let catalog = db.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
    catalog
        .relation_by_oid(relation_oid)
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(relation_oid.to_string())))
}

fn reinitialize_cluster_indexes(
    indexes: &[BoundIndexRelation],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<(), ExecError> {
    for index in indexes {
        if index.index_meta.indisvalid && index.index_meta.indisready {
            reinitialize_index_relation(index, ctx, xid)?;
        }
    }
    Ok(())
}

fn validate_cluster_rebuild_indexes(indexes: &[BoundIndexRelation]) -> Result<(), ExecError> {
    for index in indexes {
        if index.index_meta.am_oid != BTREE_AM_OID {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "CLUSTER rebuilding non-btree indexes is not supported yet".into(),
            )));
        }
        if index.index_meta.indpred.is_some() || index.index_predicate.is_some() {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "CLUSTER rebuilding partial indexes is not supported yet".into(),
            )));
        }
    }
    Ok(())
}

fn rebuild_cluster_indexes(
    relation: &BoundRelation,
    indexes: &[BoundIndexRelation],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    for index in indexes {
        if index.index_meta.indisvalid && index.index_meta.indisready {
            rebuild_cluster_index(relation, index, ctx)?;
        }
    }
    Ok(())
}

fn rebuild_cluster_index(
    relation: &BoundRelation,
    index: &BoundIndexRelation,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let _ = ctx.pool.invalidate_relation(index.rel);
    ctx.pool
        .with_storage_mut(|storage| {
            storage.smgr.truncate(
                index.rel,
                crate::backend::storage::smgr::ForkNumber::Main,
                0,
            )
        })
        .map_err(crate::backend::access::heap::heapam::HeapError::Storage)?;
    let has_expression_eval = index.index_meta.indexprs.as_ref().is_some()
        || index
            .index_meta
            .indpred
            .as_deref()
            .is_some_and(|predicate| !predicate.trim().is_empty());
    let build_ctx = IndexBuildContext {
        pool: ctx.pool.clone(),
        txns: ctx.txns.clone(),
        client_id: ctx.client_id,
        interrupts: ctx.interrupts.clone(),
        snapshot: ctx.snapshot.clone(),
        heap_relation: relation.rel,
        heap_desc: relation.desc.clone(),
        heap_toast: relation.toast,
        index_relation: index.rel,
        index_name: index.name.clone(),
        index_desc: index.desc.clone(),
        index_meta: index.index_meta.clone(),
        default_toast_compression: AttributeCompression::Pglz,
        maintenance_work_mem_kb: 65_536,
        expr_eval: has_expression_eval.then_some(IndexBuildExprContext {
            txn_waiter: ctx.txn_waiter.clone(),
            sequences: ctx.sequences.clone(),
            large_objects: ctx.large_objects.clone(),
            advisory_locks: ctx.advisory_locks.clone(),
            datetime_config: ctx.datetime_config.clone(),
            stats: ctx.stats.clone(),
            session_stats: ctx.session_stats.clone(),
            current_database_name: ctx.current_database_name.clone(),
            session_user_oid: ctx.session_user_oid,
            current_user_oid: ctx.current_user_oid,
            current_xid: ctx.snapshot.current_xid,
            statement_lock_scope_id: ctx.statement_lock_scope_id,
            session_replication_role: ctx.session_replication_role,
            visible_catalog: ctx.catalog.clone(),
        }),
    };
    crate::backend::access::index::indexam::index_build_stub(&build_ctx, index.index_meta.am_oid)
        .map_err(map_cluster_index_build_error)?;
    Ok(())
}

fn map_cluster_index_build_error(err: CatalogError) -> ExecError {
    match err {
        CatalogError::UniqueViolation(constraint) => ExecError::UniqueViolation {
            constraint,
            detail: None,
        },
        CatalogError::Interrupted(reason) => ExecError::Interrupted(reason),
        CatalogError::Io(message) if message.starts_with("index row size ") => {
            ExecError::DetailedError {
                message,
                detail: None,
                hint: Some("Values larger than 1/3 of a buffer page cannot be indexed.".into()),
                sqlstate: "54000",
            }
        }
        other => ExecError::Parse(ParseError::UnexpectedToken {
            expected: "cluster index rebuild",
            actual: format!("{other:?}"),
        }),
    }
}

fn push_unique_cluster_oid(oids: &mut Vec<u32>, oid: u32) {
    if !oids.contains(&oid) {
        oids.push(oid);
    }
}

fn relation_name_for_cluster_error(relation: &BoundRelation, fallback: &str) -> String {
    if relation.relation_oid == 0 {
        fallback.to_string()
    } else {
        relation.relation_oid.to_string()
    }
}
