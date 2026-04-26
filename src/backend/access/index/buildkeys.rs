use crate::backend::catalog::CatalogError;
use crate::backend::commands::tablecmds::{
    coerce_index_key_to_opckeytype, index_key_values_for_row,
};
use crate::backend::executor::value_io::{decode_value, missing_column_value};
use crate::backend::executor::{ExecError, ExecutorContext};
use crate::backend::parser::{
    BoundIndexRelation, relation_get_index_expressions, relation_get_index_predicate,
};
use crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot;
use crate::include::access::amapi::IndexBuildContext;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::TupleSlot;
use crate::include::nodes::primnodes::RelationDesc;

pub(crate) struct IndexBuildKeyProjector {
    bound_index: Option<BoundIndexRelation>,
    compiled_predicate: Option<crate::backend::executor::exec_expr::CompiledPredicate>,
    exec_ctx: Option<ExecutorContext>,
}

pub(crate) fn materialize_heap_row_values(
    heap_desc: &RelationDesc,
    datums: &[Option<&[u8]>],
) -> Result<Vec<Value>, CatalogError> {
    let mut row_values = Vec::with_capacity(heap_desc.columns.len());
    for (index, column) in heap_desc.columns.iter().enumerate() {
        row_values.push(if let Some(datum) = datums.get(index) {
            decode_value(column, *datum)
                .map_err(|err| CatalogError::Io(format!("heap decode failed: {err:?}")))?
        } else {
            missing_column_value(column)
        });
    }
    Ok(row_values)
}

pub(crate) fn project_index_key_values(
    index_desc: &RelationDesc,
    indkey: &[i16],
    row_values: &[Value],
    expr_values: &[Value],
) -> Result<Vec<Value>, CatalogError> {
    project_index_key_values_with_opckeytypes(index_desc, indkey, 0, &[], row_values, expr_values)
}

fn project_index_key_values_with_opckeytypes(
    index_desc: &RelationDesc,
    indkey: &[i16],
    am_oid: u32,
    opckeytype_oids: &[u32],
    row_values: &[Value],
    expr_values: &[Value],
) -> Result<Vec<Value>, CatalogError> {
    let mut keys = Vec::with_capacity(index_desc.columns.len());
    let mut expr_iter = expr_values.iter();
    for (key_pos, attnum) in indkey.iter().enumerate() {
        let value = if *attnum > 0 {
            let idx = attnum.saturating_sub(1) as usize;
            row_values
                .get(idx)
                .cloned()
                .ok_or(CatalogError::Corrupt("index key attnum out of range"))?
        } else {
            expr_iter.next().cloned().ok_or(CatalogError::Corrupt(
                "missing projected index expression value",
            ))?
        };
        keys.push(coerce_index_key_to_opckeytype(
            value,
            am_oid,
            opckeytype_oids.get(key_pos).copied(),
        ));
    }
    Ok(keys)
}

impl IndexBuildKeyProjector {
    pub(crate) fn new(ctx: &IndexBuildContext) -> Result<Self, CatalogError> {
        let has_expression_keys = ctx.index_meta.indexprs.as_ref().is_some();
        let has_predicate = ctx
            .index_meta
            .indpred
            .as_deref()
            .is_some_and(|predicate| !predicate.trim().is_empty());
        if !has_expression_keys && !has_predicate {
            return Ok(Self {
                bound_index: None,
                compiled_predicate: None,
                exec_ctx: None,
            });
        }
        let expr_ctx = ctx.expr_eval.as_ref().ok_or_else(|| {
            CatalogError::Io("index build missing expression evaluation context".into())
        })?;
        let catalog = expr_ctx.visible_catalog.as_ref().ok_or_else(|| {
            CatalogError::Io("index build missing visible catalog for index evaluation".into())
        })?;
        let mut index_meta = ctx.index_meta.clone();
        let index_exprs = relation_get_index_expressions(&mut index_meta, &ctx.heap_desc, catalog)
            .map_err(|err| CatalogError::Io(format!("index expression bind failed: {err:?}")))?;
        let index_predicate =
            relation_get_index_predicate(&mut index_meta, &ctx.heap_desc, catalog)
                .map_err(|err| CatalogError::Io(format!("index predicate bind failed: {err:?}")))?;
        let compiled_predicate = index_predicate
            .as_ref()
            .map(crate::backend::executor::exec_expr::compile_predicate);
        Ok(Self {
            bound_index: Some(BoundIndexRelation {
                name: ctx.index_name.clone(),
                rel: ctx.index_relation,
                relation_oid: ctx.index_meta.indexrelid,
                desc: ctx.index_desc.clone(),
                index_meta,
                index_exprs,
                index_predicate,
                constraint_oid: None,
                constraint_name: None,
                constraint_deferrable: false,
                constraint_initially_deferred: false,
            }),
            compiled_predicate,
            exec_ctx: Some(ExecutorContext {
                pool: ctx.pool.clone(),
                txns: ctx.txns.clone(),
                txn_waiter: expr_ctx.txn_waiter.clone(),
                lock_status_provider: None,
                sequences: expr_ctx.sequences.clone(),
                large_objects: expr_ctx.large_objects.clone(),
                stats_import_runtime: None,
                async_notify_runtime: None,
                advisory_locks: expr_ctx.advisory_locks.clone(),
                row_locks: std::sync::Arc::new(crate::backend::storage::lmgr::RowLockManager::new()),
                checkpoint_stats: CheckpointStatsSnapshot::default(),
                datetime_config: expr_ctx.datetime_config.clone(),
                statement_timestamp_usecs:
                    crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
                gucs: std::collections::HashMap::new(),
                interrupts: ctx.interrupts.clone(),
                stats: expr_ctx.stats.clone(),
                session_stats: expr_ctx.session_stats.clone(),
                snapshot: ctx.snapshot.clone(),
                transaction_state: None,
                client_id: ctx.client_id,
                current_database_name: expr_ctx.current_database_name.clone(),
                session_user_oid: expr_ctx.session_user_oid,
                current_user_oid: expr_ctx.current_user_oid,
                active_role_oid: None,
                session_replication_role: expr_ctx.session_replication_role,
                statement_lock_scope_id: expr_ctx.statement_lock_scope_id,
                transaction_lock_scope_id: None,
                next_command_id: ctx.snapshot.current_cid,
                expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                case_test_values: Vec::new(),
                system_bindings: Vec::new(),
                subplans: Vec::new(),
                timed: false,
                allow_side_effects: false,
                pending_async_notifications: Vec::new(),
                pending_catalog_effects: Vec::new(),
                pending_table_locks: Vec::new(),
                catalog: expr_ctx.visible_catalog.clone(),
                compiled_functions: std::collections::HashMap::new(),
                cte_tables: std::collections::HashMap::new(),
                cte_producers: std::collections::HashMap::new(),
                recursive_worktables: std::collections::HashMap::new(),
                deferred_foreign_keys: None,
                trigger_depth: 0,
                default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            }),
        })
    }

    pub(crate) fn project(
        &mut self,
        ctx: &IndexBuildContext,
        row_values: &[Value],
        heap_tid: ItemPointerData,
    ) -> Result<Option<Vec<Value>>, CatalogError> {
        if let (Some(bound_index), Some(exec_ctx)) =
            (self.bound_index.as_ref(), self.exec_ctx.as_mut())
        {
            if let Some(predicate) = self.compiled_predicate.as_ref() {
                let mut slot = TupleSlot::virtual_row_with_metadata(
                    row_values.to_vec(),
                    Some(heap_tid),
                    Some(ctx.index_meta.indrelid),
                );
                if !predicate(&mut slot, exec_ctx).map_err(map_build_exec_error)? {
                    return Ok(None);
                }
            }
            index_key_values_for_row(bound_index, &ctx.heap_desc, row_values, exec_ctx)
                .map(Some)
                .map_err(map_build_exec_error)
        } else {
            project_index_key_values_with_opckeytypes(
                &ctx.index_desc,
                &ctx.index_meta.indkey,
                ctx.index_meta.am_oid,
                &ctx.index_meta.opckeytype_oids,
                row_values,
                &[],
            )
            .map(Some)
        }
    }
}

fn map_build_exec_error(err: ExecError) -> CatalogError {
    match err {
        ExecError::Interrupted(reason) => CatalogError::Interrupted(reason),
        other => CatalogError::Io(format!(
            "index build expression evaluation failed: {other:?}"
        )),
    }
}
