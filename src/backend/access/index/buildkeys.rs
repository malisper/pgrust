use crate::backend::catalog::CatalogError;
use crate::backend::commands::tablecmds::index_key_values_for_row;
use crate::backend::executor::value_io::{decode_value, missing_column_value};
use crate::backend::executor::{ExecError, ExecutorContext};
use crate::backend::parser::{BoundIndexRelation, bind_index_exprs};
use crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot;
use crate::include::access::amapi::IndexBuildContext;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::RelationDesc;

pub(crate) struct IndexBuildKeyProjector {
    bound_index: Option<BoundIndexRelation>,
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
    let mut keys = Vec::with_capacity(index_desc.columns.len());
    let mut expr_iter = expr_values.iter();
    for attnum in indkey {
        if *attnum > 0 {
            let idx = attnum.saturating_sub(1) as usize;
            keys.push(
                row_values
                    .get(idx)
                    .cloned()
                    .ok_or(CatalogError::Corrupt("index key attnum out of range"))?,
            );
        } else {
            keys.push(expr_iter.next().cloned().ok_or(CatalogError::Corrupt(
                "missing projected index expression value",
            ))?);
        }
    }
    Ok(keys)
}

impl IndexBuildKeyProjector {
    pub(crate) fn new(ctx: &IndexBuildContext) -> Result<Self, CatalogError> {
        let Some(_) = ctx.index_meta.indexprs.as_ref() else {
            return Ok(Self {
                bound_index: None,
                exec_ctx: None,
            });
        };
        let expr_ctx = ctx.expr_eval.as_ref().ok_or_else(|| {
            CatalogError::Io("index build missing expression evaluation context".into())
        })?;
        let catalog = expr_ctx.visible_catalog.as_ref().ok_or_else(|| {
            CatalogError::Io("index build missing visible catalog for expression keys".into())
        })?;
        let index_exprs = bind_index_exprs(&ctx.index_meta, &ctx.heap_desc, catalog)
            .map_err(|err| CatalogError::Io(format!("index expression bind failed: {err:?}")))?;
        Ok(Self {
            bound_index: Some(BoundIndexRelation {
                name: ctx.index_name.clone(),
                rel: ctx.index_relation,
                relation_oid: ctx.index_meta.indexrelid,
                desc: ctx.index_desc.clone(),
                index_meta: ctx.index_meta.clone(),
                index_exprs,
            }),
            exec_ctx: Some(ExecutorContext {
                pool: ctx.pool.clone(),
                txns: ctx.txns.clone(),
                txn_waiter: expr_ctx.txn_waiter.clone(),
                sequences: expr_ctx.sequences.clone(),
                large_objects: expr_ctx.large_objects.clone(),
                checkpoint_stats: CheckpointStatsSnapshot::default(),
                datetime_config: expr_ctx.datetime_config.clone(),
                interrupts: ctx.interrupts.clone(),
                stats: expr_ctx.stats.clone(),
                session_stats: expr_ctx.session_stats.clone(),
                snapshot: ctx.snapshot.clone(),
                client_id: ctx.client_id,
                session_user_oid: expr_ctx.session_user_oid,
                current_user_oid: expr_ctx.current_user_oid,
                active_role_oid: None,
                next_command_id: ctx.snapshot.current_cid,
                expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                case_test_values: Vec::new(),
                system_bindings: Vec::new(),
                subplans: Vec::new(),
                timed: false,
                allow_side_effects: false,
                catalog: expr_ctx.visible_catalog.clone(),
                compiled_functions: std::collections::HashMap::new(),
                cte_tables: std::collections::HashMap::new(),
                cte_producers: std::collections::HashMap::new(),
                recursive_worktables: std::collections::HashMap::new(),
                deferred_foreign_keys: None,
                default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            }),
        })
    }

    pub(crate) fn project(
        &mut self,
        ctx: &IndexBuildContext,
        row_values: &[Value],
    ) -> Result<Vec<Value>, CatalogError> {
        if let (Some(bound_index), Some(exec_ctx)) =
            (self.bound_index.as_ref(), self.exec_ctx.as_mut())
        {
            index_key_values_for_row(bound_index, &ctx.heap_desc, row_values, exec_ctx)
                .map_err(map_build_exec_error)
        } else {
            project_index_key_values(&ctx.index_desc, &ctx.index_meta.indkey, row_values, &[])
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
