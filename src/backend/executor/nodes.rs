use super::{
    AggGroup, ExecError, ExecutorContext, OrderedAggInput, build_aggregate_runtime, executor_start,
};
use crate::backend::access::heap::heapam::{
    heap_fetch_visible_with_txns, heap_scan_begin_visible, heap_scan_end,
    heap_scan_page_next_tuple, heap_scan_prepare_next_page,
};
use crate::backend::access::index::indexam;
use crate::backend::commands::explain::format_explain_lines_with_costs;
use crate::backend::executor::exec_expr::{compare_order_by_keys, eval_expr};
use crate::backend::executor::expr_geometry::render_geometry_text;
use crate::backend::executor::pg_regex::explain_similar_pattern;
use crate::backend::executor::srf::{
    eval_project_set_returning_call, eval_set_returning_call, set_returning_call_label,
};
use crate::backend::executor::value_io::{decode_value_with_toast, missing_column_value};
use crate::backend::executor::window::execute_window_clause;
use crate::backend::libpq::pqformat::FloatFormatOptions;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::storage::lmgr::RowLockMode;
use crate::backend::storage::page::bufpage::{
    ItemIdFlags, page_get_item_id_unchecked, page_get_item_unchecked, page_get_max_offset_number,
};
use crate::backend::storage::smgr::ForkNumber;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::time::date::format_date_text;
use crate::backend::utils::time::instant::Instant;
use crate::include::access::scankey::ScanKeyData;
use crate::include::catalog::PG_LARGEOBJECT_METADATA_RELATION_OID;
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::{
    AggregateState, AppendState, BitmapHeapScanState, BitmapIndexScanState, CteScanState,
    FilterState, FunctionScanState, IndexScanState, LimitState, LockRowsState, MaterializedRow,
    NestedLoopJoinState, NodeExecStats, OrderByState, PlanNode, PlanState, ProjectSetState,
    ProjectionState, RecursiveUnionState, ResultState, SeqScanState, SetOpState, SlotKind,
    SubqueryScanState, SystemVarBinding, ToastRelationRef, TupleSlot, ValuesState, WindowAggState,
    WorkTableScanState,
};
use crate::include::nodes::plannodes::{IndexScanKey, IndexScanKeyArgument};
use crate::include::nodes::primnodes::{
    BuiltinScalarFunction, Expr, FuncExpr, INDEX_VAR, INNER_VAR, JoinType, OUTER_VAR, ParamKind,
    RelationDesc, ScalarFunctionImpl, Var, attrno_index,
};
use std::cell::RefCell;
use std::collections::{BTreeSet, HashSet};
use std::rc::Rc;

const EMPTY_SYSTEM_BINDINGS: [SystemVarBinding; 0] = [];

fn slot_toast_context(
    relation: Option<ToastRelationRef>,
    ctx: &ExecutorContext,
) -> Option<crate::include::nodes::execnodes::ToastFetchContext> {
    relation.map(
        |relation| crate::include::nodes::execnodes::ToastFetchContext {
            relation,
            pool: ctx.pool.clone(),
            txns: ctx.txns.clone(),
            snapshot: ctx.snapshot.clone(),
            client_id: ctx.client_id,
        },
    )
}

fn eval_bool_qual(
    expr: &Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    match eval_expr(expr, slot, ctx)? {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) | Value::Null => Ok(false),
        other => Err(ExecError::NonBoolQual(other)),
    }
}

fn eval_qual_list(
    quals: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    for qual in quals {
        if !eval_bool_qual(qual, slot, ctx)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn set_active_system_bindings(ctx: &mut ExecutorContext, bindings: &[SystemVarBinding]) {
    ctx.system_bindings.clear();
    ctx.system_bindings.extend_from_slice(bindings);
}

fn set_outer_expr_bindings(
    ctx: &mut ExecutorContext,
    values: Vec<Value>,
    bindings: &[SystemVarBinding],
) {
    ctx.expr_bindings.outer_tuple = Some(values);
    ctx.expr_bindings.outer_system_bindings = bindings.to_vec();
}

fn clear_outer_expr_bindings(ctx: &mut ExecutorContext) {
    ctx.expr_bindings.outer_tuple = None;
    ctx.expr_bindings.outer_system_bindings.clear();
}

fn set_inner_expr_bindings(
    ctx: &mut ExecutorContext,
    values: Vec<Value>,
    bindings: &[SystemVarBinding],
) {
    ctx.expr_bindings.inner_tuple = Some(values);
    ctx.expr_bindings.inner_system_bindings = bindings.to_vec();
}

fn clear_inner_expr_bindings(ctx: &mut ExecutorContext) {
    ctx.expr_bindings.inner_tuple = None;
    ctx.expr_bindings.inner_system_bindings.clear();
}

fn materialize_slot_values(slot: &mut TupleSlot) -> Result<Vec<Value>, ExecError> {
    let mut values = slot.values()?.to_vec();
    Value::materialize_all(&mut values);
    Ok(values)
}

fn sequence_scan_runtime(
    ctx: &ExecutorContext,
) -> Result<&crate::pgrust::database::SequenceRuntime, ExecError> {
    ctx.sequences
        .as_deref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "sequence runtime unavailable".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn large_object_runtime(
    ctx: &ExecutorContext,
) -> Result<&crate::pgrust::database::LargeObjectRuntime, ExecError> {
    ctx.large_objects
        .as_deref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "large object runtime unavailable".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn bind_exec_params(
    params: &[crate::include::nodes::plannodes::ExecParamSource],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<(usize, Option<Value>)>, ExecError> {
    let mut saved = Vec::with_capacity(params.len());
    for param in params {
        let value = eval_expr(&param.expr, slot, ctx)?;
        let old = ctx.expr_bindings.exec_params.insert(param.paramid, value);
        saved.push((param.paramid, old));
    }
    Ok(saved)
}

fn restore_exec_params(saved: Vec<(usize, Option<Value>)>, ctx: &mut ExecutorContext) {
    for (paramid, old) in saved {
        if let Some(value) = old {
            ctx.expr_bindings.exec_params.insert(paramid, value);
        } else {
            ctx.expr_bindings.exec_params.remove(&paramid);
        }
    }
}

fn merge_system_bindings(
    left: &[SystemVarBinding],
    right: &[SystemVarBinding],
) -> Vec<SystemVarBinding> {
    let mut merged = left.to_vec();
    for binding in right {
        if !merged
            .iter()
            .any(|existing| existing.varno == binding.varno)
        {
            merged.push(*binding);
        }
    }
    merged
}

fn format_qual_list(quals: &[Expr]) -> Expr {
    let mut quals = quals.to_vec();
    let first = quals.remove(0);
    quals
        .into_iter()
        .fold(first, |acc, qual| Expr::and(acc, qual))
}

fn finish_row(stats: &mut NodeExecStats, start: Option<Instant>) {
    if stats.first_tuple_time.is_none() {
        stats.first_tuple_time = Some(start.map(|start| start.elapsed()).unwrap_or_default());
    }
    stats.rows += 1;
    if let Some(start) = start {
        stats.total_time += start.elapsed();
    }
}

fn finish_eof(stats: &mut NodeExecStats, start: Option<Instant>, ctx: &ExecutorContext) {
    stats.loops += 1;
    if let Some(start) = start {
        stats.total_time += start.elapsed();
    }
    if let Some(start_usage) = stats.buffer_usage_start.take() {
        let end_usage = ctx.pool.usage_stats();
        stats.buffer_usage.shared_hit = end_usage.shared_hit.saturating_sub(start_usage.shared_hit);
        stats.buffer_usage.shared_read = end_usage
            .shared_read
            .saturating_sub(start_usage.shared_read);
        stats.buffer_usage.shared_written = end_usage
            .shared_written
            .saturating_sub(start_usage.shared_written);
    }
}

fn begin_node(stats: &mut NodeExecStats, ctx: &ExecutorContext) {
    if stats.buffer_usage_start.is_none() {
        stats.buffer_usage_start = Some(ctx.pool.usage_stats());
    }
}

fn note_filtered_row(stats: &mut NodeExecStats) {
    stats.rows_removed_by_filter += 1;
}

fn materialize_cte_row(
    slot: &mut TupleSlot,
    bindings: &[SystemVarBinding],
) -> Result<MaterializedRow, ExecError> {
    let mut values = slot.values()?.to_vec();
    Value::materialize_all(&mut values);
    Ok(MaterializedRow::new(
        TupleSlot::virtual_row_with_metadata(values, slot.tid(), slot.table_oid),
        bindings.to_vec(),
    ))
}

fn row_lock_read_only_error(mode: RowLockMode) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "{} is not allowed in a read-only execution context",
            mode.pg_name()
        ),
        detail: None,
        hint: None,
        sqlstate: "25006",
    }
}

fn lock_current_row_marks(
    ctx: &ExecutorContext,
    row_marks: &[crate::include::nodes::plannodes::PlanRowMark],
    bindings: &[SystemVarBinding],
) -> Result<(), ExecError> {
    if row_marks.is_empty() {
        return Ok(());
    }
    if !ctx.allow_side_effects {
        let mode = row_marks
            .first()
            .map(|row_mark| RowLockMode::from_select_locking_clause(row_mark.strength))
            .unwrap_or(RowLockMode::Exclusive);
        return Err(row_lock_read_only_error(mode));
    }

    let mut seen = BTreeSet::new();
    for row_mark in row_marks {
        let binding = bindings
            .iter()
            .find(|binding| binding.varno == row_mark.rtindex)
            .ok_or_else(|| {
                internal_exec_error(format!(
                    "missing system binding for row-marked relation {}",
                    row_mark.relation_name
                ))
            })?;
        let tid = binding.tid.ok_or_else(|| {
            internal_exec_error(format!(
                "missing tuple identity for row-marked relation {}",
                row_mark.relation_name
            ))
        })?;
        if !seen.insert((row_mark.relation_oid, tid)) {
            continue;
        }
        ctx.acquire_row_lock(
            row_mark.relation_oid,
            tid,
            RowLockMode::from_select_locking_clause(row_mark.strength),
        )?;
    }
    Ok(())
}

fn internal_exec_error(message: impl Into<String>) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    }
}

fn bitmap_am_error(operation: &'static str, err: impl std::fmt::Debug) -> ExecError {
    ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
        expected: operation,
        actual: format!("{err:?}"),
    })
}

fn eval_index_scan_key_argument(
    argument: &IndexScanKeyArgument,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let value = match argument {
        IndexScanKeyArgument::Const(value) => value.clone(),
        IndexScanKeyArgument::Runtime(expr) => eval_expr(expr, slot, ctx)?,
    };
    Ok(value.to_owned_value())
}

fn eval_index_scan_keys(
    keys: &[IndexScanKey],
    ctx: &mut ExecutorContext,
    null_short_circuits: bool,
) -> Result<Option<Vec<ScanKeyData>>, ExecError> {
    let mut slot = TupleSlot::empty(0);
    let mut scan_keys = Vec::with_capacity(keys.len());
    for key in keys {
        let argument = eval_index_scan_key_argument(&key.argument, &mut slot, ctx)?;
        if null_short_circuits
            && matches!(key.argument, IndexScanKeyArgument::Runtime(_))
            && matches!(argument, Value::Null)
        {
            return Ok(None);
        }
        scan_keys.push(key.to_scan_key(argument));
    }
    Ok(Some(scan_keys))
}

fn collect_visible_page_offsets(
    page: &crate::backend::storage::buffer::Page,
    snapshot: &crate::backend::utils::time::snapmgr::Snapshot,
    txns: &parking_lot::RwLock<crate::backend::access::transam::xact::TransactionManager>,
) -> Result<Vec<u16>, ExecError> {
    let max_offset =
        page_get_max_offset_number(page).map_err(crate::include::access::htup::TupleError::from)?;
    let txns_guard = txns.read();
    let mut offsets = Vec::new();
    for off in 1..=max_offset {
        let item_id = page_get_item_id_unchecked(page, off);
        if item_id.lp_flags != ItemIdFlags::Normal || !item_id.has_storage() {
            continue;
        }
        let tuple_bytes = page_get_item_unchecked(page, off);
        if snapshot.tuple_bytes_visible(&txns_guard, tuple_bytes) {
            offsets.push(off);
        }
    }
    Ok(offsets)
}

fn set_op_result_rows(
    op: crate::include::nodes::parsenodes::SetOperator,
    children: &mut [PlanState],
    ctx: &mut ExecutorContext,
) -> Result<Vec<MaterializedRow>, ExecError> {
    #[derive(Clone)]
    struct Bucket {
        row: MaterializedRow,
        counts: Vec<usize>,
    }

    let child_count = children.len();
    let mut child_rows = Vec::with_capacity(children.len());
    let mut buckets: Vec<Bucket> = Vec::new();

    for (child_index, child) in children.iter_mut().enumerate() {
        let mut rows = Vec::new();
        while child.exec_proc_node(ctx)?.is_some() {
            let row = child.materialize_current_row()?;
            let values = row.slot.tts_values.clone();
            rows.push(row.clone());

            if let Some(bucket) = buckets
                .iter_mut()
                .find(|bucket| bucket.row.slot.tts_values == values)
            {
                bucket.counts[child_index] += 1;
            } else {
                let mut counts = vec![0; child_count];
                counts[child_index] = 1;
                buckets.push(Bucket { row, counts });
            }
        }
        child_rows.push(rows);
    }

    let mut result = Vec::new();
    match op {
        crate::include::nodes::parsenodes::SetOperator::Union { all: true } => {
            for rows in child_rows {
                result.extend(rows);
            }
        }
        crate::include::nodes::parsenodes::SetOperator::Union { all: false } => {
            for bucket in buckets {
                result.push(bucket.row);
            }
        }
        crate::include::nodes::parsenodes::SetOperator::Intersect { all } => {
            for bucket in buckets {
                let repeats = if all {
                    bucket.counts.iter().copied().min().unwrap_or(0)
                } else if bucket.counts.iter().all(|count| *count > 0) {
                    1
                } else {
                    0
                };
                result.extend(std::iter::repeat_n(bucket.row, repeats));
            }
        }
        crate::include::nodes::parsenodes::SetOperator::Except { all } => {
            for bucket in buckets {
                let repeats = if all {
                    bucket.counts[0].saturating_sub(bucket.counts.iter().skip(1).sum::<usize>())
                } else if bucket.counts[0] > 0
                    && bucket.counts.iter().skip(1).all(|count| *count == 0)
                {
                    1
                } else {
                    0
                };
                result.extend(std::iter::repeat_n(bucket.row, repeats));
            }
        }
    }

    Ok(result)
}

fn load_materialized_row(
    slot: &mut TupleSlot,
    row: &MaterializedRow,
    bindings: &mut Vec<SystemVarBinding>,
    ctx: &mut ExecutorContext,
) {
    *slot = row.slot.clone();
    bindings.clear();
    bindings.extend_from_slice(&row.system_bindings);
    set_active_system_bindings(ctx, bindings);
}

impl PlanNode for ResultState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx);
        begin_node(&mut self.stats, ctx);
        if self.emitted {
            finish_eof(&mut self.stats, start, ctx);
            Ok(None)
        } else {
            self.emitted = true;
            self.slot.store_virtual_row(Vec::new(), None, None);
            ctx.system_bindings.clear();
            finish_row(&mut self.stats, start);
            Ok(Some(&mut self.slot))
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &EMPTY_SYSTEM_BINDINGS
    }
    fn column_names(&self) -> &[String] {
        &[]
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Result".into()
    }
    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

impl PlanNode for AppendState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx);
        while self.current_child < self.children.len() {
            if let Some(slot) = self.children[self.current_child].exec_proc_node(ctx)? {
                let child_bindings = ctx.system_bindings.clone();
                let mut values = slot.values()?.to_vec();
                Value::materialize_all(&mut values);
                self.current_bindings = child_bindings
                    .first()
                    .map(|binding| {
                        vec![SystemVarBinding {
                            varno: self.source_id,
                            table_oid: binding.table_oid,
                            tid: binding.tid,
                        }]
                    })
                    .unwrap_or_default();
                let table_oid = self
                    .current_bindings
                    .first()
                    .map(|binding| binding.table_oid);
                self.slot.store_virtual_row(values, slot.tid(), table_oid);
                set_active_system_bindings(ctx, &self.current_bindings);
                finish_row(&mut self.stats, start);
                return Ok(Some(&mut self.slot));
            }
            self.current_child += 1;
        }
        finish_eof(&mut self.stats, start, ctx);
        Ok(None)
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn column_names(&self) -> &[String] {
        &self.column_names
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Append".into()
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        for child in &self.children {
            format_explain_lines_with_costs(child.as_ref(), indent + 1, analyze, show_costs, lines);
        }
    }
}

impl PlanNode for SeqScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.relation_oid == PG_LARGEOBJECT_METADATA_RELATION_OID {
            let start = if ctx.timed {
                Some(Instant::now())
            } else {
                None
            };
            begin_node(&mut self.stats, ctx);
            if self.scan_rows.is_empty() {
                self.scan_rows = large_object_runtime(ctx)?.metadata_rows();
            }
            loop {
                let Some(values) = self.scan_rows.get(self.scan_index).cloned() else {
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                };
                self.scan_index += 1;
                self.slot
                    .store_virtual_row(values, None, Some(self.relation_oid));
                self.current_bindings = vec![SystemVarBinding {
                    varno: self.source_id,
                    table_oid: self.relation_oid,
                    tid: None,
                }];
                set_active_system_bindings(ctx, &self.current_bindings);
                if let Some(qual) = &self.qual {
                    let outer_values = materialize_slot_values(&mut self.slot)?;
                    let current_bindings = self.current_bindings.clone();
                    set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                    clear_inner_expr_bindings(ctx);
                    if !qual(&mut self.slot, ctx)? {
                        note_filtered_row(&mut self.stats);
                        continue;
                    }
                }
                finish_row(&mut self.stats, start);
                return Ok(Some(&mut self.slot));
            }
        }
        if self.relkind == 'S' {
            let start = if ctx.timed {
                Some(Instant::now())
            } else {
                None
            };
            begin_node(&mut self.stats, ctx);
            if self.sequence_emitted {
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }

            let values = sequence_scan_runtime(ctx)?
                .current_row(self.relation_oid)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("sequence {} does not exist", self.relation_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42P01",
                })?;
            self.slot
                .store_virtual_row(values, None, Some(self.relation_oid));
            self.current_bindings = vec![SystemVarBinding {
                varno: self.source_id,
                table_oid: self.relation_oid,
                tid: None,
            }];
            set_active_system_bindings(ctx, &self.current_bindings);

            if let Some(qual) = &self.qual {
                let outer_values = materialize_slot_values(&mut self.slot)?;
                let current_bindings = self.current_bindings.clone();
                set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                clear_inner_expr_bindings(ctx);
                if !qual(&mut self.slot, ctx)? {
                    self.sequence_emitted = true;
                    note_filtered_row(&mut self.stats);
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                }
            }

            self.sequence_emitted = true;
            finish_row(&mut self.stats, start);
            return Ok(Some(&mut self.slot));
        }

        if self.scan.is_none() {
            self.scan = Some(heap_scan_begin_visible(
                &ctx.pool,
                ctx.client_id,
                self.rel,
                ctx.snapshot.clone(),
            )?);
            ctx.session_stats
                .write()
                .note_relation_scan(self.relation_oid);
        }

        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx);

        loop {
            ctx.check_for_interrupts()?;
            let scan = self.scan.as_mut().unwrap();
            if scan.has_page_tuples() {
                let buffer_id = scan.pinned_buffer_id().expect("buffer must be pinned");
                let page = unsafe { ctx.pool.page_unlocked(buffer_id) }
                    .expect("pinned buffer must be valid");

                if let Some((tid, tuple_bytes)) = heap_scan_page_next_tuple(page, scan) {
                    let raw_ptr = tuple_bytes.as_ptr();
                    let raw_len = tuple_bytes.len();
                    let pin = scan.pinned_buffer_rc().expect("buffer must be pinned");

                    self.slot.kind = SlotKind::BufferHeapTuple {
                        desc: self.desc.clone(),
                        attr_descs: self.attr_descs.clone(),
                        tid,
                        tuple_ptr: raw_ptr,
                        tuple_len: raw_len,
                        pin,
                    };
                    self.slot.toast = slot_toast_context(self.toast_relation, ctx);
                    self.slot.tts_nvalid = 0;
                    self.slot.tts_values.clear();
                    self.slot.decode_offset = 0;
                    self.slot.table_oid = Some(self.relation_oid);
                    self.current_bindings = vec![SystemVarBinding {
                        varno: self.source_id,
                        table_oid: self.relation_oid,
                        tid: Some(tid),
                    }];
                    set_active_system_bindings(ctx, &self.current_bindings);

                    if let Some(qual) = &self.qual {
                        let outer_values = materialize_slot_values(&mut self.slot)?;
                        let current_bindings = self.current_bindings.clone();
                        set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                        clear_inner_expr_bindings(ctx);
                        if !qual(&mut self.slot, ctx)? {
                            note_filtered_row(&mut self.stats);
                            continue;
                        }
                    }

                    ctx.session_stats
                        .write()
                        .note_relation_tuple_returned(self.relation_oid);
                    finish_row(&mut self.stats, start);
                    return Ok(Some(&mut self.slot));
                }
            }

            let next: Result<Option<usize>, ExecError> =
                heap_scan_prepare_next_page(&*ctx.pool, ctx.client_id, &ctx.txns, scan);
            if next?.is_none() {
                heap_scan_end::<ExecError>(&*ctx.pool, ctx.client_id, scan)?;
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            } else {
                let mut session_stats = ctx.session_stats.write();
                session_stats.note_relation_block_fetched(self.relation_oid);
                session_stats.note_io_read("client backend", "relation", "bulkread", 8192);
            }
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn column_names(&self) -> &[String] {
        &self.column_names
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        format!("Seq Scan on {}", self.relation_name)
    }
    fn explain_details(
        &self,
        indent: usize,
        analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        if let Some(qual_expr) = &self.qual_expr {
            let prefix = "  ".repeat(indent + 1);
            lines.push(format!(
                "{prefix}Filter: {}",
                render_explain_expr(qual_expr, &self.column_names)
            ));
        }
        let prefix = "  ".repeat(indent + 1);
        if analyze && self.stats.rows_removed_by_filter > 0 {
            lines.push(format!(
                "{prefix}Rows Removed by Filter: {}",
                self.stats.rows_removed_by_filter
            ));
        }
        if analyze
            && (self.stats.buffer_usage.shared_hit > 0
                || self.stats.buffer_usage.shared_read > 0
                || self.stats.buffer_usage.shared_written > 0)
        {
            lines.push(format!(
                "{prefix}{}",
                crate::backend::commands::explain::format_buffer_usage(self.stats.buffer_usage)
            ));
        }
    }
    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

impl PlanNode for IndexScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        if self.scan_exhausted {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        }
        if self.scan.is_none() {
            let Some(key_data) = eval_index_scan_keys(&self.keys, ctx, true)? else {
                self.scan_exhausted = true;
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            };
            let order_by_data =
                eval_index_scan_keys(&self.order_by_keys, ctx, false)?.unwrap_or_default();
            let begin = crate::include::access::amapi::IndexBeginScanContext {
                pool: ctx.pool.clone(),
                client_id: ctx.client_id,
                snapshot: ctx.snapshot.clone(),
                heap_relation: self.rel,
                index_relation: self.index_rel,
                index_desc: (*self.index_desc).clone(),
                index_meta: self.index_meta.clone(),
                key_data,
                order_by_data,
                direction: self.direction,
                want_itup: false,
            };
            self.scan = Some(
                indexam::index_beginscan(&begin, self.am_oid).map_err(|err| {
                    ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                        expected: "index access method begin scan",
                        actual: format!("{err:?}"),
                    })
                })?,
            );
            let mut session_stats = ctx.session_stats.write();
            session_stats.note_relation_scan(self.index_meta.indexrelid);
            session_stats.note_io_read("client backend", "relation", "normal", 8192);
        }

        loop {
            ctx.check_for_interrupts()?;
            let has_tuple = {
                let scan = self.scan.as_mut().expect("index scan must exist");
                indexam::index_getnext(scan, self.am_oid).map_err(|err| {
                    ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                        expected: "index access method tuple",
                        actual: format!("{err:?}"),
                    })
                })?
            };
            if !has_tuple {
                if let Some(scan) = self.scan.take() {
                    indexam::index_endscan(scan, self.am_oid).map_err(|err| {
                        ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                            expected: "index access method end scan",
                            actual: format!("{err:?}"),
                        })
                    })?;
                }
                self.scan_exhausted = true;
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }

            let tid = self
                .scan
                .as_ref()
                .and_then(|scan| scan.xs_heaptid)
                .expect("index scan tuple must set heap tid");
            {
                let mut session_stats = ctx.session_stats.write();
                session_stats.note_relation_tuple_returned(self.index_meta.indexrelid);
                session_stats.note_relation_block_fetched(self.index_meta.indexrelid);
            }
            let visible = heap_fetch_visible_with_txns(
                &ctx.pool,
                ctx.client_id,
                self.rel,
                tid,
                &ctx.txns,
                &ctx.snapshot,
            )?;
            let Some(tuple) = visible else {
                continue;
            };
            {
                let mut session_stats = ctx.session_stats.write();
                session_stats.note_relation_tuple_fetched(self.relation_oid);
                session_stats.note_relation_block_fetched(self.relation_oid);
                session_stats.note_io_read("client backend", "relation", "normal", 8192);
            }
            self.slot.kind = SlotKind::HeapTuple {
                desc: self.desc.clone(),
                attr_descs: self.attr_descs.clone(),
                tid,
                tuple,
            };
            self.slot.toast = slot_toast_context(self.toast_relation, ctx);
            self.slot.tts_nvalid = 0;
            self.slot.tts_values.clear();
            self.slot.decode_offset = 0;
            self.slot.table_oid = Some(self.relation_oid);
            self.current_bindings = vec![SystemVarBinding {
                varno: self.source_id,
                table_oid: self.relation_oid,
                tid: Some(tid),
            }];
            set_active_system_bindings(ctx, &self.current_bindings);

            finish_row(&mut self.stats, start);
            return Ok(Some(&mut self.slot));
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn column_names(&self) -> &[String] {
        &self.column_names
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        format!(
            "Index Scan using {} on {}",
            self.index_name, self.relation_name
        )
    }
    fn explain_details(
        &self,
        indent: usize,
        _analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        if self.keys.is_empty() {
            return;
        }
        let rendered = self
            .keys
            .iter()
            .filter_map(|key| render_index_scan_key(key, &self.desc, &self.index_meta))
            .collect::<Vec<_>>();
        if rendered.is_empty() {
            return;
        }
        let prefix = "  ".repeat(indent + 1);
        let detail = if rendered.len() == 1 {
            rendered[0].clone()
        } else {
            format!("({})", rendered.join(" AND "))
        };
        lines.push(format!("{prefix}Index Cond: ({detail})"));
    }
    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

fn render_index_scan_key(
    key: &IndexScanKey,
    desc: &RelationDesc,
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
) -> Option<String> {
    let index_attno = usize::try_from(key.attribute_number.checked_sub(1)?).ok()?;
    let heap_attno = usize::try_from(*index_meta.indkey.get(index_attno)?)
        .ok()?
        .checked_sub(1)?;
    let column_name = desc.columns.get(heap_attno)?.name.clone();
    let right_type_oid = match &key.argument {
        IndexScanKeyArgument::Const(value) => value
            .sql_type_hint()
            .map(crate::backend::utils::cache::catcache::sql_type_oid),
        IndexScanKeyArgument::Runtime(expr) => {
            crate::include::nodes::primnodes::expr_sql_type_hint(expr)
                .map(crate::backend::utils::cache::catcache::sql_type_oid)
        }
    };
    let operator = index_meta
        .amop_entries
        .get(index_attno)?
        .iter()
        .filter(|entry| {
            entry.purpose == 's' && u16::try_from(entry.strategy).ok() == Some(key.strategy)
        })
        .filter(|entry| {
            right_type_oid.is_none()
                || Some(entry.righttype) == right_type_oid
                || entry.righttype == crate::include::catalog::ANYOID
        })
        .max_by_key(|entry| {
            if Some(entry.righttype) == right_type_oid {
                2
            } else if entry.righttype == crate::include::catalog::ANYOID {
                1
            } else {
                0
            }
        })?;
    let operator_name = crate::include::catalog::bootstrap_pg_operator_rows()
        .into_iter()
        .find(|row| row.oid == operator.operator_oid)
        .map(|row| row.oprname)
        .unwrap_or_else(|| format!("op{}", operator.operator_oid));
    let value_sql = match &key.argument {
        IndexScanKeyArgument::Const(value) => match right_type_oid {
            Some(_type_oid) => {
                let sql_type = value.sql_type_hint()?;
                format!(
                    "{}::{}",
                    render_explain_literal(value),
                    render_explain_sql_type_name(sql_type)
                )
            }
            None => render_explain_literal(value),
        },
        IndexScanKeyArgument::Runtime(expr) => render_explain_expr(expr, &[]),
    };
    Some(format!("{column_name} {operator_name} {value_sql}"))
}

impl BitmapIndexScanState {
    fn fill_bitmap(&mut self, ctx: &mut ExecutorContext) -> Result<(), ExecError> {
        if self.executed {
            return Ok(());
        }

        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx);

        let Some(key_data) = eval_index_scan_keys(&self.keys, ctx, true)? else {
            self.executed = true;
            self.stats.rows = 0;
            finish_eof(&mut self.stats, start, ctx);
            return Ok(());
        };
        let begin = crate::include::access::amapi::IndexBeginScanContext {
            pool: ctx.pool.clone(),
            client_id: ctx.client_id,
            snapshot: ctx.snapshot.clone(),
            heap_relation: self.rel,
            index_relation: self.index_rel,
            index_desc: (*self.index_desc).clone(),
            index_meta: self.index_meta.clone(),
            key_data,
            order_by_data: Vec::new(),
            direction: crate::include::access::relscan::ScanDirection::Forward,
            want_itup: false,
        };
        let mut scan = indexam::index_beginscan(&begin, self.am_oid)
            .map_err(|err| bitmap_am_error("index access method begin bitmap scan", err))?;
        {
            let mut session_stats = ctx.session_stats.write();
            session_stats.note_relation_scan(self.index_meta.indexrelid);
            session_stats.note_io_read("client backend", "relation", "normal", 8192);
        }
        let tuples = indexam::index_getbitmap(&mut scan, self.am_oid, &mut self.bitmap)
            .map_err(|err| bitmap_am_error("index access method bitmap scan", err))?;
        indexam::index_endscan(scan, self.am_oid)
            .map_err(|err| bitmap_am_error("index access method end bitmap scan", err))?;

        self.executed = true;
        self.stats.rows = tuples.max(0) as u64;
        finish_eof(&mut self.stats, start, ctx);
        Ok(())
    }
}

impl PlanNode for BitmapIndexScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        _ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        Err(internal_exec_error(
            "bitmap index scan cannot produce tuples directly",
        ))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        None
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &EMPTY_SYSTEM_BINDINGS
    }

    fn column_names(&self) -> &[String] {
        &self.column_names
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        format!(
            "Bitmap Index Scan using rel {} on rel {}",
            self.index_rel.rel_number, self.rel.rel_number
        )
    }

    fn explain_details(
        &self,
        indent: usize,
        analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        if !self.index_quals.is_empty() {
            let prefix = "  ".repeat(indent + 1);
            lines.push(format!(
                "{prefix}Index Cond: {}",
                render_explain_expr(&format_qual_list(&self.index_quals), &self.column_names)
            ));
        }
        if analyze
            && (self.stats.buffer_usage.shared_hit > 0
                || self.stats.buffer_usage.shared_read > 0
                || self.stats.buffer_usage.shared_written > 0)
        {
            let prefix = "  ".repeat(indent + 1);
            lines.push(format!(
                "{prefix}{}",
                crate::backend::commands::explain::format_buffer_usage(self.stats.buffer_usage)
            ));
        }
    }

    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

impl BitmapHeapScanState {
    fn load_next_bitmap_page(&mut self, ctx: &mut ExecutorContext) -> Result<bool, ExecError> {
        self.current_page_offsets.clear();
        self.current_offset_index = 0;
        self.current_page_pin = None;

        while let Some(block) = self.bitmap_pages.get(self.current_page_index).copied() {
            self.current_page_index += 1;

            let pin = ctx
                .pool
                .pin_existing_block(ctx.client_id, self.rel, ForkNumber::Main, block)
                .map_err(|err| {
                    internal_exec_error(format!("bitmap heap pin block failed: {err:?}"))
                })?;
            let buffer_id = pin.into_raw();
            let owned_pin =
                crate::OwnedBufferPin::wrap_existing(std::sync::Arc::clone(&ctx.pool), buffer_id);
            let pin_rc = Rc::new(owned_pin);

            let guard = ctx.pool.lock_buffer_shared(buffer_id).map_err(|err| {
                internal_exec_error(format!("bitmap heap shared lock failed: {err:?}"))
            })?;
            let mut offsets = collect_visible_page_offsets(&guard, &ctx.snapshot, &ctx.txns)?;
            drop(guard);
            if let Some(exact_offsets) = self.bitmap_index.bitmap.exact_offsets(block) {
                offsets.retain(|offset| exact_offsets.contains(offset));
            }

            if offsets.is_empty() {
                continue;
            }

            {
                let mut session_stats = ctx.session_stats.write();
                session_stats.note_relation_block_fetched(self.relation_oid);
                session_stats.note_io_read("client backend", "relation", "normal", 8192);
            }

            self.current_page_pin = Some(pin_rc);
            self.current_page_offsets = offsets;
            return Ok(true);
        }

        Ok(false)
    }
}

impl PlanNode for BitmapHeapScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx);

        if self.bitmap_pages.is_empty() && self.current_page_index == 0 {
            self.bitmap_index.fill_bitmap(ctx)?;
            self.bitmap_pages = self.bitmap_index.bitmap.iter().collect();
        }

        loop {
            ctx.check_for_interrupts()?;

            if self.current_offset_index >= self.current_page_offsets.len() {
                if !self.load_next_bitmap_page(ctx)? {
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                }
            }

            let offset = self.current_page_offsets[self.current_offset_index];
            self.current_offset_index += 1;
            let pin = self
                .current_page_pin
                .as_ref()
                .cloned()
                .ok_or_else(|| internal_exec_error("bitmap heap scan lost current page pin"))?;
            let buffer_id = pin.buffer_id();
            let page = unsafe { ctx.pool.page_unlocked(buffer_id) }
                .ok_or_else(|| internal_exec_error("bitmap heap scan page vanished"))?;
            let tuple_bytes = page_get_item_unchecked(page, offset);

            self.slot.kind = SlotKind::BufferHeapTuple {
                desc: self.desc.clone(),
                attr_descs: self.attr_descs.clone(),
                tid: crate::include::access::itemptr::ItemPointerData {
                    block_number: self.bitmap_pages[self.current_page_index - 1],
                    offset_number: offset,
                },
                tuple_ptr: tuple_bytes.as_ptr(),
                tuple_len: tuple_bytes.len(),
                pin,
            };
            self.slot.toast = slot_toast_context(self.toast_relation, ctx);
            self.slot.tts_nvalid = 0;
            self.slot.tts_values.clear();
            self.slot.decode_offset = 0;
            self.slot.table_oid = Some(self.relation_oid);
            self.current_bindings = vec![SystemVarBinding {
                varno: self.source_id,
                table_oid: self.relation_oid,
                tid: Some(crate::include::access::itemptr::ItemPointerData {
                    block_number: self.bitmap_pages[self.current_page_index - 1],
                    offset_number: offset,
                }),
            }];
            set_active_system_bindings(ctx, &self.current_bindings);

            if let Some(recheck) = &self.compiled_recheck {
                let outer_values = materialize_slot_values(&mut self.slot)?;
                let current_bindings = self.current_bindings.clone();
                set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                clear_inner_expr_bindings(ctx);
                if !recheck(&mut self.slot, ctx)? {
                    note_filtered_row(&mut self.stats);
                    continue;
                }
            }

            {
                let mut session_stats = ctx.session_stats.write();
                session_stats.note_relation_tuple_returned(self.relation_oid);
                session_stats.note_relation_tuple_fetched(self.relation_oid);
            }
            finish_row(&mut self.stats, start);
            return Ok(Some(&mut self.slot));
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.column_names
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        format!("Bitmap Heap Scan on {}", self.relation_name)
    }

    fn explain_details(
        &self,
        indent: usize,
        analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        if let Some(recheck_qual) = &self.recheck_qual {
            let prefix = "  ".repeat(indent + 1);
            lines.push(format!(
                "{prefix}Recheck Cond: {}",
                render_explain_expr(recheck_qual, &self.column_names)
            ));
        }
        let prefix = "  ".repeat(indent + 1);
        if analyze && self.stats.rows_removed_by_filter > 0 {
            lines.push(format!(
                "{prefix}Rows Removed by Recheck: {}",
                self.stats.rows_removed_by_filter
            ));
        }
        if analyze
            && (self.stats.buffer_usage.shared_hit > 0
                || self.stats.buffer_usage.shared_read > 0
                || self.stats.buffer_usage.shared_written > 0)
        {
            lines.push(format!(
                "{prefix}{}",
                crate::backend::commands::explain::format_buffer_usage(self.stats.buffer_usage)
            ));
        }
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            self.bitmap_index.as_ref(),
            indent + 1,
            analyze,
            show_costs,
            lines,
        );
    }
}

pub(crate) fn render_explain_expr(expr: &Expr, column_names: &[String]) -> String {
    render_explain_expr_with_qualifier(expr, None, column_names)
}

pub(crate) fn render_explain_expr_with_qualifier(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    format!(
        "({})",
        render_explain_expr_inner_with_qualifier(expr, qualifier, column_names)
    )
}

pub(crate) fn render_explain_projection_expr_with_qualifier(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    format!(
        "({})",
        render_explain_projection_expr_inner_with_qualifier(expr, qualifier, column_names)
    )
}

pub(crate) fn render_explain_join_expr(
    expr: &Expr,
    outer_names: &[String],
    inner_names: &[String],
) -> String {
    format!(
        "({})",
        render_explain_join_expr_inner(expr, outer_names, inner_names)
    )
}

fn render_explain_var_name(var: &Var, column_names: &[String]) -> Option<String> {
    attrno_index(var.varattno).and_then(|index| column_names.get(index).cloned())
}

fn render_explain_expr_inner(expr: &Expr, column_names: &[String]) -> String {
    render_explain_expr_inner_with_qualifier(expr, None, column_names)
}

fn render_explain_expr_inner_with_qualifier(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    match expr {
        Expr::Var(var) => render_explain_var_name(var, column_names)
            .map(|name| match qualifier {
                Some(qualifier) => format!("{qualifier}.{name}"),
                None => name,
            })
            .unwrap_or_else(|| format!("{expr:?}")),
        Expr::Param(param) if param.paramkind == ParamKind::Exec => {
            format!("${}", param.paramid)
        }
        Expr::Const(value) => render_explain_const(value),
        Expr::Cast(inner, ty) => render_explain_cast(inner, *ty, qualifier, column_names),
        Expr::Op(op) => match op.op {
            crate::include::nodes::primnodes::OpExprKind::Add
            | crate::include::nodes::primnodes::OpExprKind::Sub
            | crate::include::nodes::primnodes::OpExprKind::Mul
            | crate::include::nodes::primnodes::OpExprKind::Div
            | crate::include::nodes::primnodes::OpExprKind::Mod
            | crate::include::nodes::primnodes::OpExprKind::BitAnd
            | crate::include::nodes::primnodes::OpExprKind::BitOr
            | crate::include::nodes::primnodes::OpExprKind::BitXor
            | crate::include::nodes::primnodes::OpExprKind::Shl
            | crate::include::nodes::primnodes::OpExprKind::Shr
            | crate::include::nodes::primnodes::OpExprKind::Concat => {
                let [left, right] = op.args.as_slice() else {
                    return format!("{expr:?}");
                };
                let op_text = match op.op {
                    crate::include::nodes::primnodes::OpExprKind::Add => "+",
                    crate::include::nodes::primnodes::OpExprKind::Sub => "-",
                    crate::include::nodes::primnodes::OpExprKind::Mul => "*",
                    crate::include::nodes::primnodes::OpExprKind::Div => "/",
                    crate::include::nodes::primnodes::OpExprKind::Mod => "%",
                    crate::include::nodes::primnodes::OpExprKind::BitAnd => "&",
                    crate::include::nodes::primnodes::OpExprKind::BitOr => "|",
                    crate::include::nodes::primnodes::OpExprKind::BitXor => "#",
                    crate::include::nodes::primnodes::OpExprKind::Shl => "<<",
                    crate::include::nodes::primnodes::OpExprKind::Shr => ">>",
                    crate::include::nodes::primnodes::OpExprKind::Concat => "||",
                    _ => unreachable!(),
                };
                format!(
                    "{} {} {}",
                    render_explain_infix_operand(left, qualifier, column_names),
                    op_text,
                    render_explain_infix_operand(right, qualifier, column_names)
                )
            }
            crate::include::nodes::primnodes::OpExprKind::Eq
            | crate::include::nodes::primnodes::OpExprKind::NotEq
            | crate::include::nodes::primnodes::OpExprKind::Lt
            | crate::include::nodes::primnodes::OpExprKind::LtEq
            | crate::include::nodes::primnodes::OpExprKind::Gt
            | crate::include::nodes::primnodes::OpExprKind::GtEq
            | crate::include::nodes::primnodes::OpExprKind::RegexMatch => {
                let [left, right] = op.args.as_slice() else {
                    return format!("{expr:?}");
                };
                let op_text = match op.op {
                    crate::include::nodes::primnodes::OpExprKind::Eq => "=",
                    crate::include::nodes::primnodes::OpExprKind::NotEq => "<>",
                    crate::include::nodes::primnodes::OpExprKind::Lt => "<",
                    crate::include::nodes::primnodes::OpExprKind::LtEq => "<=",
                    crate::include::nodes::primnodes::OpExprKind::Gt => ">",
                    crate::include::nodes::primnodes::OpExprKind::GtEq => ">=",
                    crate::include::nodes::primnodes::OpExprKind::RegexMatch => "~",
                    _ => unreachable!(),
                };
                format!(
                    "{} {} {}",
                    render_explain_infix_operand(left, qualifier, column_names),
                    op_text,
                    render_explain_infix_operand(right, qualifier, column_names)
                )
            }
            _ => format!("{expr:?}"),
        },
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            crate::include::nodes::primnodes::BoolExprType::And => {
                let rendered = bool_expr
                    .args
                    .iter()
                    .map(|arg| {
                        render_explain_expr_inner_with_qualifier(arg, qualifier, column_names)
                    })
                    .collect::<Vec<_>>();
                format!("({})", rendered.join(" AND "))
            }
            crate::include::nodes::primnodes::BoolExprType::Or => {
                let rendered = bool_expr
                    .args
                    .iter()
                    .map(|arg| {
                        render_explain_expr_inner_with_qualifier(arg, qualifier, column_names)
                    })
                    .collect::<Vec<_>>();
                format!("({})", rendered.join(" OR "))
            }
            crate::include::nodes::primnodes::BoolExprType::Not => {
                let Some(inner) = bool_expr.args.first() else {
                    return format!("{expr:?}");
                };
                format!(
                    "NOT {}",
                    render_explain_expr_inner_with_qualifier(inner, qualifier, column_names)
                )
            }
        },
        Expr::Coalesce(left, right) => format!(
            "COALESCE({}, {})",
            render_explain_expr_inner_with_qualifier(left, qualifier, column_names),
            render_explain_expr_inner_with_qualifier(right, qualifier, column_names)
        ),
        Expr::IsNull(inner) => {
            format!(
                "{} IS NULL",
                render_explain_expr_inner_with_qualifier(inner, qualifier, column_names)
            )
        }
        Expr::IsNotNull(inner) => {
            format!(
                "{} IS NOT NULL",
                render_explain_expr_inner_with_qualifier(inner, qualifier, column_names)
            )
        }
        Expr::Func(func) => render_explain_func_expr(func, qualifier, column_names),
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            ..
        } => render_similar_explain_expr(expr, pattern, escape.as_deref(), *negated, |expr| {
            render_explain_expr_inner_with_qualifier(expr, qualifier, column_names)
        }),
        other => format!("{other:?}"),
    }
}

fn render_explain_func_expr(
    func: &FuncExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    if let Some(operator) = builtin_scalar_function_infix_operator(func.implementation) {
        if let [left, right] = func.args.as_slice() {
            return format!(
                "{} {} {}",
                render_explain_infix_operand(left, qualifier, column_names),
                operator,
                render_explain_infix_operand(right, qualifier, column_names)
            );
        }
    }
    format!("{func:?}")
}

fn builtin_scalar_function_infix_operator(
    implementation: ScalarFunctionImpl,
) -> Option<&'static str> {
    match implementation {
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoSame) => Some("~="),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoDistance) => Some("<->"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoContains) => Some("@>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoContainedBy) => Some("<@"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoOverlap) => Some("&&"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoLeft) => Some("<<"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoOverLeft) => Some("&<"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoRight) => Some(">>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoOverRight) => Some("&>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoBelow) => Some("<<|"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoOverBelow) => Some("&<|"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoAbove) => Some("|>>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoOverAbove) => Some("|&>"),
        _ => None,
    }
}

pub(crate) fn render_explain_projection_expr_inner_with_qualifier(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    match expr {
        Expr::Var(var) => render_explain_var_name(var, column_names)
            .map(|name| match qualifier {
                Some(qualifier) => format!("{qualifier}.{name}"),
                None => name,
            })
            .unwrap_or_else(|| format!("{expr:?}")),
        Expr::Const(value) => render_explain_projection_const(value),
        Expr::Op(op) => match op.op {
            crate::include::nodes::primnodes::OpExprKind::Add
            | crate::include::nodes::primnodes::OpExprKind::Sub
            | crate::include::nodes::primnodes::OpExprKind::Mul
            | crate::include::nodes::primnodes::OpExprKind::Div
            | crate::include::nodes::primnodes::OpExprKind::Mod
            | crate::include::nodes::primnodes::OpExprKind::BitAnd
            | crate::include::nodes::primnodes::OpExprKind::BitOr
            | crate::include::nodes::primnodes::OpExprKind::BitXor
            | crate::include::nodes::primnodes::OpExprKind::Shl
            | crate::include::nodes::primnodes::OpExprKind::Shr
            | crate::include::nodes::primnodes::OpExprKind::Concat => {
                let [left, right] = op.args.as_slice() else {
                    return render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
                };
                let op_text = match op.op {
                    crate::include::nodes::primnodes::OpExprKind::Add => "+",
                    crate::include::nodes::primnodes::OpExprKind::Sub => "-",
                    crate::include::nodes::primnodes::OpExprKind::Mul => "*",
                    crate::include::nodes::primnodes::OpExprKind::Div => "/",
                    crate::include::nodes::primnodes::OpExprKind::Mod => "%",
                    crate::include::nodes::primnodes::OpExprKind::BitAnd => "&",
                    crate::include::nodes::primnodes::OpExprKind::BitOr => "|",
                    crate::include::nodes::primnodes::OpExprKind::BitXor => "#",
                    crate::include::nodes::primnodes::OpExprKind::Shl => "<<",
                    crate::include::nodes::primnodes::OpExprKind::Shr => ">>",
                    crate::include::nodes::primnodes::OpExprKind::Concat => "||",
                    _ => unreachable!(),
                };
                format!(
                    "{} {} {}",
                    render_explain_projection_expr_inner_with_qualifier(
                        left,
                        qualifier,
                        column_names,
                    ),
                    op_text,
                    render_explain_projection_expr_inner_with_qualifier(
                        right,
                        qualifier,
                        column_names,
                    )
                )
            }
            _ => render_explain_expr_inner_with_qualifier(expr, qualifier, column_names),
        },
        _ => render_explain_expr_inner_with_qualifier(expr, qualifier, column_names),
    }
}

fn render_explain_infix_operand(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    match expr {
        Expr::Const(_) | Expr::Cast(_, _) => {
            render_explain_expr_inner_with_qualifier(expr, qualifier, column_names)
        }
        _ => render_explain_expr_inner_with_qualifier(expr, qualifier, column_names),
    }
}

pub(crate) fn render_explain_join_expr_inner(
    expr: &Expr,
    outer_names: &[String],
    inner_names: &[String],
) -> String {
    match expr {
        Expr::Var(var) if var.varno == OUTER_VAR => {
            render_explain_var_name(var, outer_names).unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Var(var) if var.varno == INNER_VAR => {
            render_explain_var_name(var, inner_names).unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Var(var) if var.varno == INDEX_VAR => {
            render_explain_var_name(var, inner_names).unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Var(var) => {
            let mut combined_names = outer_names.to_vec();
            combined_names.extend_from_slice(inner_names);
            render_explain_var_name(var, &combined_names).unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Const(value) => render_explain_const(value),
        Expr::Cast(inner, ty) => render_explain_join_cast(inner, *ty, outer_names, inner_names),
        Expr::Op(op) => match op.op {
            crate::include::nodes::primnodes::OpExprKind::Eq
            | crate::include::nodes::primnodes::OpExprKind::NotEq
            | crate::include::nodes::primnodes::OpExprKind::Lt
            | crate::include::nodes::primnodes::OpExprKind::LtEq
            | crate::include::nodes::primnodes::OpExprKind::Gt
            | crate::include::nodes::primnodes::OpExprKind::GtEq
            | crate::include::nodes::primnodes::OpExprKind::RegexMatch => {
                let [left, right] = op.args.as_slice() else {
                    return format!("{expr:?}");
                };
                let op_text = match op.op {
                    crate::include::nodes::primnodes::OpExprKind::Eq => "=",
                    crate::include::nodes::primnodes::OpExprKind::NotEq => "<>",
                    crate::include::nodes::primnodes::OpExprKind::Lt => "<",
                    crate::include::nodes::primnodes::OpExprKind::LtEq => "<=",
                    crate::include::nodes::primnodes::OpExprKind::Gt => ">",
                    crate::include::nodes::primnodes::OpExprKind::GtEq => ">=",
                    crate::include::nodes::primnodes::OpExprKind::RegexMatch => "~",
                    _ => unreachable!(),
                };
                format!(
                    "{} {} {}",
                    render_explain_join_expr_inner(left, outer_names, inner_names),
                    op_text,
                    render_explain_join_expr_inner(right, outer_names, inner_names)
                )
            }
            _ => format!("{expr:?}"),
        },
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            crate::include::nodes::primnodes::BoolExprType::And => {
                let rendered = bool_expr
                    .args
                    .iter()
                    .map(|arg| render_explain_join_expr_inner(arg, outer_names, inner_names))
                    .collect::<Vec<_>>();
                format!("({})", rendered.join(" AND "))
            }
            crate::include::nodes::primnodes::BoolExprType::Or => {
                let rendered = bool_expr
                    .args
                    .iter()
                    .map(|arg| render_explain_join_expr_inner(arg, outer_names, inner_names))
                    .collect::<Vec<_>>();
                format!("({})", rendered.join(" OR "))
            }
            crate::include::nodes::primnodes::BoolExprType::Not => {
                let Some(inner) = bool_expr.args.first() else {
                    return format!("{expr:?}");
                };
                format!(
                    "NOT {}",
                    render_explain_join_expr_inner(inner, outer_names, inner_names)
                )
            }
        },
        Expr::Coalesce(left, right) => format!(
            "COALESCE({}, {})",
            render_explain_join_expr_inner(left, outer_names, inner_names),
            render_explain_join_expr_inner(right, outer_names, inner_names)
        ),
        Expr::IsNull(inner) => {
            format!(
                "{} IS NULL",
                render_explain_join_expr_inner(inner, outer_names, inner_names)
            )
        }
        Expr::IsNotNull(inner) => format!(
            "{} IS NOT NULL",
            render_explain_join_expr_inner(inner, outer_names, inner_names)
        ),
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            ..
        } => render_similar_explain_expr(expr, pattern, escape.as_deref(), *negated, |expr| {
            render_explain_join_expr_inner(expr, outer_names, inner_names)
        }),
        other => format!("{other:?}"),
    }
}

fn render_similar_explain_expr<F>(
    expr: &Expr,
    pattern: &Expr,
    escape: Option<&Expr>,
    negated: bool,
    render: F,
) -> String
where
    F: Fn(&Expr) -> String,
{
    let left = render(expr);
    if let Some(regex) = explain_similar_regex(pattern, escape) {
        let op = if negated { "!~" } else { "~" };
        return format!(
            "{} {} {}",
            left,
            op,
            render_explain_const(&Value::Text(regex.into()))
        );
    }

    let keyword = if negated {
        "NOT SIMILAR TO"
    } else {
        "SIMILAR TO"
    };
    let mut out = format!("{} {} {}", left, keyword, render(pattern));
    if let Some(escape) = escape {
        out.push_str(" ESCAPE ");
        out.push_str(&render(escape));
    }
    out
}

fn explain_similar_regex(pattern: &Expr, escape: Option<&Expr>) -> Option<String> {
    let Expr::Const(pattern) = pattern else {
        return None;
    };
    let pattern = pattern.as_text()?;
    let escape = match escape {
        None => None,
        Some(Expr::Const(Value::Null)) => return None,
        Some(Expr::Const(value)) => Some(value.as_text()?),
        Some(_) => return None,
    };
    explain_similar_pattern(pattern, escape).ok()
}

fn render_explain_const(value: &Value) -> String {
    match value {
        Value::Text(_) | Value::TextRef(_, _) => {
            format!("'{}'::text", value.as_text().unwrap().replace('\'', "''"))
        }
        Value::Date(date) => format!(
            "'{}'::date",
            format_date_text(*date, &DateTimeConfig::default())
        ),
        Value::Int16(v) => format!("{v}::smallint"),
        Value::Int32(v) => format!("{v}::integer"),
        Value::Int64(v) => format!("{v}::bigint"),
        Value::Bool(v) => format!("{}::boolean", if *v { "true" } else { "false" }),
        Value::Null => "NULL".to_string(),
        other => format!("{other:?}"),
    }
}

fn render_explain_projection_const(value: &Value) -> String {
    match value {
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        _ => render_explain_const(value),
    }
}

fn render_explain_cast(
    expr: &Expr,
    ty: SqlType,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    if let Expr::Const(value) = expr {
        return format!(
            "{}::{}",
            render_explain_literal(value),
            render_explain_sql_type_name(ty)
        );
    }
    let inner = render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    format!("({inner})::{}", render_explain_sql_type_name(ty))
}

fn render_explain_join_cast(
    expr: &Expr,
    ty: SqlType,
    outer_names: &[String],
    inner_names: &[String],
) -> String {
    if let Expr::Const(value) = expr {
        return format!(
            "{}::{}",
            render_explain_literal(value),
            render_explain_sql_type_name(ty)
        );
    }
    let inner = render_explain_join_expr_inner(expr, outer_names, inner_names);
    format!("({inner})::{}", render_explain_sql_type_name(ty))
}

fn render_explain_literal(value: &Value) -> String {
    match value {
        Value::Text(_) | Value::TextRef(_, _) => {
            format!("'{}'", value.as_text().unwrap().replace('\'', "''"))
        }
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => {
            let rendered = render_geometry_text(value, FloatFormatOptions::default())
                .unwrap_or_else(|| format!("{value:?}"));
            format!("'{rendered}'")
        }
        Value::Date(date) => {
            format!("'{}'", format_date_text(*date, &DateTimeConfig::default()))
        }
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Bool(v) => {
            if *v {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Value::Null => "NULL".to_string(),
        other => format!("{other:?}"),
    }
}

fn render_explain_sql_type_name(ty: SqlType) -> &'static str {
    if ty.is_array {
        return "array";
    }
    match ty.kind {
        SqlTypeKind::Bool => "boolean",
        SqlTypeKind::Int2 => "smallint",
        SqlTypeKind::Int4 => "integer",
        SqlTypeKind::Int8 => "bigint",
        SqlTypeKind::Float4 => "real",
        SqlTypeKind::Float8 => "double precision",
        SqlTypeKind::Numeric => "numeric",
        SqlTypeKind::Text => "text",
        SqlTypeKind::Name => "name",
        SqlTypeKind::Oid => "oid",
        SqlTypeKind::Date => "date",
        SqlTypeKind::Json => "json",
        SqlTypeKind::Jsonb => "jsonb",
        SqlTypeKind::Box => "box",
        SqlTypeKind::Point => "point",
        _ => "text",
    }
}

impl PlanNode for FilterState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx);
        loop {
            ctx.check_for_interrupts()?;
            let slot = match self.input.exec_proc_node(ctx)? {
                Some(s) => s,
                None => {
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                }
            };
            let outer_values = materialize_slot_values(slot)?;
            let current_bindings = ctx.system_bindings.clone();
            set_outer_expr_bindings(ctx, outer_values, &current_bindings);
            clear_inner_expr_bindings(ctx);

            if (self.compiled_predicate)(slot, ctx)? {
                finish_row(&mut self.stats, start);
                return Ok(self.input.current_slot());
            }
            note_filtered_row(&mut self.stats);
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        self.input.current_slot()
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        self.input.current_system_bindings()
    }
    fn column_names(&self) -> &[String] {
        self.input.column_names()
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Filter".into()
    }
    fn explain_details(
        &self,
        indent: usize,
        analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = "  ".repeat(indent + 1);
        lines.push(format!(
            "{prefix}Filter: {}",
            render_explain_expr(&self.predicate, self.column_names())
        ));
        if analyze && self.stats.rows_removed_by_filter > 0 {
            lines.push(format!(
                "{prefix}Rows Removed by Filter: {}",
                self.stats.rows_removed_by_filter
            ));
        }
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(&*self.input, indent + 1, analyze, show_costs, lines);
    }
}

impl PlanNode for NestedLoopJoinState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        if self.right_plan.is_some() {
            return exec_lateral_join(self, ctx, start);
        }
        if matches!(self.kind, JoinType::Cross) && self.cross_right_outer {
            return exec_cross_join(self, ctx, start);
        }

        if self.right_rows.is_none() {
            let mut rows = Vec::new();
            while self.right.exec_proc_node(ctx)?.is_some() {
                ctx.check_for_interrupts()?;
                rows.push(self.right.materialize_current_row()?);
            }
            self.right_matched = Some(vec![false; rows.len()]);
            self.right_rows = Some(rows);
        }

        loop {
            ctx.check_for_interrupts()?;
            if self.current_left.is_none() {
                match self.left.exec_proc_node(ctx)?.is_some() {
                    true => {
                        self.current_left = Some(self.left.materialize_current_row()?);
                        self.current_left_matched = false;
                        self.right_index = 0;
                    }
                    false => {
                        if matches!(self.kind, JoinType::Right | JoinType::Full) {
                            let right_rows = self.right_rows.as_ref().unwrap();
                            let right_matched = self.right_matched.as_mut().unwrap();
                            while self.unmatched_right_index < right_rows.len() {
                                let ri = self.unmatched_right_index;
                                self.unmatched_right_index += 1;
                                if right_matched[ri] {
                                    continue;
                                }
                                let mut combined_values = vec![Value::Null; self.left_width];
                                combined_values
                                    .extend(right_rows[ri].slot.tts_values.iter().cloned());
                                self.slot.tts_values = combined_values;
                                self.slot.tts_nvalid = self.left_width + self.right_width;
                                self.slot.kind = SlotKind::Virtual;
                                self.slot.virtual_tid = None;
                                self.slot.decode_offset = 0;
                                self.current_bindings = right_rows[ri].system_bindings.clone();
                                set_active_system_bindings(ctx, &self.current_bindings);
                                finish_row(&mut self.stats, start);
                                return Ok(Some(&mut self.slot));
                            }
                        }
                        finish_eof(&mut self.stats, start, ctx);
                        return Ok(None);
                    }
                }
            }

            let right_rows = self.right_rows.as_ref().unwrap();
            let right_matched = self.right_matched.as_mut().unwrap();

            while self.right_index < right_rows.len() {
                let ri = self.right_index;
                self.right_index += 1;

                let left = self.current_left.as_ref().unwrap();
                let right = &right_rows[ri];
                let mut combined_values: Vec<Value> = left.slot.tts_values.clone();
                combined_values.extend(right.slot.tts_values.iter().cloned());
                let nvalid = combined_values.len();
                self.slot.tts_values = combined_values;
                self.slot.tts_nvalid = nvalid;
                self.slot.kind = SlotKind::Virtual;
                self.slot.virtual_tid = None;
                self.slot.decode_offset = 0;
                self.current_bindings =
                    merge_system_bindings(&left.system_bindings, &right.system_bindings);
                set_active_system_bindings(ctx, &self.current_bindings);
                set_outer_expr_bindings(ctx, left.slot.tts_values.clone(), &left.system_bindings);
                set_inner_expr_bindings(ctx, right.slot.tts_values.clone(), &right.system_bindings);

                if eval_qual_list(&self.join_qual, &mut self.slot, ctx)? {
                    self.current_left_matched = true;
                    right_matched[ri] = true;
                    if matches!(self.kind, JoinType::Anti) {
                        self.right_index = right_rows.len();
                        break;
                    }
                    if eval_qual_list(&self.qual, &mut self.slot, ctx)? {
                        if matches!(self.kind, JoinType::Semi) {
                            let left = self.current_left.take().unwrap();
                            self.slot.tts_values = left.slot.tts_values;
                            self.slot.tts_nvalid = self.left_width;
                            self.slot.kind = SlotKind::Virtual;
                            self.slot.virtual_tid = None;
                            self.slot.decode_offset = 0;
                            self.current_bindings = left.system_bindings;
                            set_active_system_bindings(ctx, &self.current_bindings);
                            finish_row(&mut self.stats, start);
                            return Ok(Some(&mut self.slot));
                        }
                        finish_row(&mut self.stats, start);
                        return Ok(Some(&mut self.slot));
                    }
                }
            }

            if !self.current_left_matched && matches!(self.kind, JoinType::Anti) {
                let left = self.current_left.take().unwrap();
                self.slot.tts_values = left.slot.tts_values;
                self.slot.tts_nvalid = self.left_width;
                self.slot.kind = SlotKind::Virtual;
                self.slot.virtual_tid = None;
                self.slot.decode_offset = 0;
                self.current_bindings = left.system_bindings;
                set_active_system_bindings(ctx, &self.current_bindings);
                finish_row(&mut self.stats, start);
                return Ok(Some(&mut self.slot));
            }

            if !self.current_left_matched && matches!(self.kind, JoinType::Left | JoinType::Full) {
                let left = self.current_left.as_ref().unwrap();
                let mut combined_values: Vec<Value> = left.slot.tts_values.clone();
                combined_values.extend(std::iter::repeat_n(Value::Null, self.right_width));
                self.slot.tts_values = combined_values;
                self.slot.tts_nvalid = self.left_width + self.right_width;
                self.slot.kind = SlotKind::Virtual;
                self.slot.virtual_tid = None;
                self.slot.decode_offset = 0;
                self.current_bindings = left.system_bindings.clone();
                set_active_system_bindings(ctx, &self.current_bindings);
                self.current_left = None;
                finish_row(&mut self.stats, start);
                return Ok(Some(&mut self.slot));
            }

            self.current_left = None;
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn column_names(&self) -> &[String] {
        &self.output_names
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        match self.kind {
            JoinType::Inner => "Nested Loop".into(),
            JoinType::Left => "Nested Loop Left Join".into(),
            JoinType::Right => "Nested Loop Right Join".into(),
            JoinType::Full => "Nested Loop Full Join".into(),
            JoinType::Semi => "Nested Loop Semi Join".into(),
            JoinType::Anti => "Nested Loop Anti Join".into(),
            JoinType::Cross => "Nested Loop".into(),
        }
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = "  ".repeat(indent + 1);
        if !self.join_qual.is_empty() {
            let (left_names, right_names) = self.combined_names.split_at(self.left_width);
            lines.push(format!(
                "{prefix}Join Filter: {}",
                render_explain_join_expr(
                    &format_qual_list(&self.join_qual),
                    left_names,
                    right_names
                )
            ));
        }
        if !self.qual.is_empty() {
            let (left_names, right_names) = self.combined_names.split_at(self.left_width);
            lines.push(format!(
                "{prefix}Filter: {}",
                render_explain_join_expr(&format_qual_list(&self.qual), left_names, right_names)
            ));
        }
        format_explain_lines_with_costs(&*self.left, indent + 1, analyze, show_costs, lines);
        format_explain_lines_with_costs(&*self.right, indent + 1, analyze, show_costs, lines);
    }
}

fn exec_lateral_join<'a>(
    state: &'a mut NestedLoopJoinState,
    ctx: &mut ExecutorContext,
    start: Option<Instant>,
) -> Result<Option<&'a mut TupleSlot>, ExecError> {
    loop {
        ctx.check_for_interrupts()?;
        if state.current_left.is_none() {
            match state.left.exec_proc_node(ctx)?.is_some() {
                true => {
                    let current_left = state.left.materialize_current_row()?;
                    let values = current_left.slot.tts_values.clone();
                    state.current_left = Some(current_left);
                    state.current_left_matched = false;
                    set_outer_expr_bindings(
                        ctx,
                        values,
                        &state.current_left.as_ref().unwrap().system_bindings,
                    );
                    let saved_params = bind_exec_params(
                        &state.nest_params,
                        &mut state.current_left.as_mut().unwrap().slot,
                        ctx,
                    )?;
                    state.right = super::executor_start(
                        state
                            .right_plan
                            .as_ref()
                            .expect("lateral right plan")
                            .clone(),
                    );
                    let mut rows = Vec::new();
                    while state.right.exec_proc_node(ctx)?.is_some() {
                        ctx.check_for_interrupts()?;
                        rows.push(state.right.materialize_current_row()?);
                    }
                    state.right_rows = Some(rows);
                    state.right_matched = Some(vec![
                        false;
                        state
                            .right_rows
                            .as_ref()
                            .expect("lateral right rows")
                            .len()
                    ]);
                    state.right_index = 0;
                    state.unmatched_right_index = 0;
                    state.current_nest_param_saves = Some(saved_params);
                }
                false => {
                    finish_eof(&mut state.stats, start, ctx);
                    return Ok(None);
                }
            }
        }

        let right_rows = state.right_rows.as_ref().unwrap();
        let right_matched = state.right_matched.as_mut().unwrap();

        while state.right_index < right_rows.len() {
            ctx.check_for_interrupts()?;
            let ri = state.right_index;
            state.right_index += 1;

            let right = &right_rows[ri];
            let right_bindings = right.system_bindings.clone();
            let values = right.slot.tts_values.clone();
            let left = state.current_left.as_ref().unwrap();
            let mut combined_values: Vec<Value> = left.slot.tts_values.clone();
            combined_values.extend(values);
            let nvalid = combined_values.len();
            state.slot.tts_values = combined_values;
            state.slot.tts_nvalid = nvalid;
            state.slot.kind = SlotKind::Virtual;
            state.slot.virtual_tid = None;
            state.slot.decode_offset = 0;
            state.current_bindings = merge_system_bindings(&left.system_bindings, &right_bindings);
            set_active_system_bindings(ctx, &state.current_bindings);
            set_outer_expr_bindings(ctx, left.slot.tts_values.clone(), &left.system_bindings);
            set_inner_expr_bindings(ctx, right.slot.tts_values.clone(), &right_bindings);

            if eval_qual_list(&state.join_qual, &mut state.slot, ctx)? {
                state.current_left_matched = true;
                right_matched[ri] = true;
                if matches!(state.kind, JoinType::Anti) {
                    state.right_index = right_rows.len();
                    break;
                }
                if eval_qual_list(&state.qual, &mut state.slot, ctx)? {
                    if matches!(state.kind, JoinType::Semi) {
                        let left = state.current_left.take().unwrap();
                        state.slot.tts_values = left.slot.tts_values;
                        state.slot.tts_nvalid = state.left_width;
                        state.slot.kind = SlotKind::Virtual;
                        state.slot.virtual_tid = None;
                        state.slot.decode_offset = 0;
                        state.current_bindings = left.system_bindings;
                        set_active_system_bindings(ctx, &state.current_bindings);
                        if let Some(saved_params) = state.current_nest_param_saves.take() {
                            restore_exec_params(saved_params, ctx);
                        }
                        state.right_rows = None;
                        state.right_matched = None;
                        finish_row(&mut state.stats, start);
                        return Ok(Some(&mut state.slot));
                    }
                    finish_row(&mut state.stats, start);
                    return Ok(Some(&mut state.slot));
                }
            }
        }

        if matches!(state.kind, JoinType::Right | JoinType::Full) {
            while state.unmatched_right_index < right_rows.len() {
                let ri = state.unmatched_right_index;
                state.unmatched_right_index += 1;
                if right_matched[ri] {
                    continue;
                }

                let right = &right_rows[ri];
                let mut combined_values = vec![Value::Null; state.left_width];
                combined_values.extend(right.slot.tts_values.iter().cloned());
                state.slot.tts_values = combined_values;
                state.slot.tts_nvalid = state.left_width + state.right_width;
                state.slot.kind = SlotKind::Virtual;
                state.slot.virtual_tid = None;
                state.slot.decode_offset = 0;
                state.current_bindings = right.system_bindings.clone();
                set_active_system_bindings(ctx, &state.current_bindings);
                finish_row(&mut state.stats, start);
                return Ok(Some(&mut state.slot));
            }
        }

        clear_outer_expr_bindings(ctx);
        clear_inner_expr_bindings(ctx);
        if !state.current_left_matched && matches!(state.kind, JoinType::Anti) {
            let left = state.current_left.take().unwrap();
            state.slot.tts_values = left.slot.tts_values;
            state.slot.tts_nvalid = state.left_width;
            state.slot.kind = SlotKind::Virtual;
            state.slot.virtual_tid = None;
            state.slot.decode_offset = 0;
            state.current_bindings = left.system_bindings;
            set_active_system_bindings(ctx, &state.current_bindings);
            if let Some(saved_params) = state.current_nest_param_saves.take() {
                restore_exec_params(saved_params, ctx);
            }
            state.right_rows = None;
            state.right_matched = None;
            finish_row(&mut state.stats, start);
            return Ok(Some(&mut state.slot));
        }
        if !state.current_left_matched && matches!(state.kind, JoinType::Left | JoinType::Full) {
            let left = state.current_left.as_ref().unwrap();
            let mut combined_values: Vec<Value> = left.slot.tts_values.clone();
            combined_values.extend(std::iter::repeat_n(Value::Null, state.right_width));
            state.slot.tts_values = combined_values;
            state.slot.tts_nvalid = state.left_width + state.right_width;
            state.slot.kind = SlotKind::Virtual;
            state.slot.virtual_tid = None;
            state.slot.decode_offset = 0;
            state.current_bindings = left.system_bindings.clone();
            set_active_system_bindings(ctx, &state.current_bindings);
            if let Some(saved_params) = state.current_nest_param_saves.take() {
                restore_exec_params(saved_params, ctx);
            }
            state.right_rows = None;
            state.right_matched = None;
            state.current_left = None;
            finish_row(&mut state.stats, start);
            return Ok(Some(&mut state.slot));
        }

        if let Some(saved_params) = state.current_nest_param_saves.take() {
            restore_exec_params(saved_params, ctx);
        }
        state.right_rows = None;
        state.right_matched = None;
        state.current_left = None;
    }
}

fn exec_cross_join<'a>(
    state: &'a mut NestedLoopJoinState,
    ctx: &mut ExecutorContext,
    start: Option<Instant>,
) -> Result<Option<&'a mut TupleSlot>, ExecError> {
    if state.left_rows.is_none() {
        let mut rows = Vec::new();
        while state.left.exec_proc_node(ctx)?.is_some() {
            ctx.check_for_interrupts()?;
            rows.push(state.left.materialize_current_row()?);
        }
        state.left_rows = Some(rows);
    }

    loop {
        ctx.check_for_interrupts()?;
        if state.current_right.is_none() {
            match state.right.exec_proc_node(ctx)?.is_some() {
                true => {
                    state.current_right = Some(state.right.materialize_current_row()?);
                    state.left_index = 0;
                }
                false => {
                    finish_eof(&mut state.stats, start, ctx);
                    return Ok(None);
                }
            }
        }

        let left_rows = state.left_rows.as_ref().unwrap();
        while state.left_index < left_rows.len() {
            let li = state.left_index;
            state.left_index += 1;

            let left = &left_rows[li];
            let right = state.current_right.as_ref().unwrap();
            let mut combined_values: Vec<Value> = left.slot.tts_values.clone();
            combined_values.extend(right.slot.tts_values.iter().cloned());
            let nvalid = combined_values.len();
            state.slot.tts_values = combined_values;
            state.slot.tts_nvalid = nvalid;
            state.slot.kind = SlotKind::Virtual;
            state.slot.virtual_tid = None;
            state.slot.decode_offset = 0;
            state.current_bindings =
                merge_system_bindings(&left.system_bindings, &right.system_bindings);
            set_active_system_bindings(ctx, &state.current_bindings);
            set_outer_expr_bindings(ctx, left.slot.tts_values.clone(), &left.system_bindings);
            set_inner_expr_bindings(ctx, right.slot.tts_values.clone(), &right.system_bindings);

            if eval_qual_list(&state.join_qual, &mut state.slot, ctx)?
                && eval_qual_list(&state.qual, &mut state.slot, ctx)?
            {
                finish_row(&mut state.stats, start);
                return Ok(Some(&mut state.slot));
            }
        }

        state.current_right = None;
    }
}

impl PlanNode for OrderByState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx);
        if self.rows.is_none() {
            let mut rows = Vec::new();
            while self.input.exec_proc_node(ctx)?.is_some() {
                ctx.check_for_interrupts()?;
                rows.push(self.input.materialize_current_row()?);
            }

            let mut keyed_rows = Vec::with_capacity(rows.len());
            for mut row in rows {
                ctx.check_for_interrupts()?;
                set_active_system_bindings(ctx, &row.system_bindings);
                set_outer_expr_bindings(ctx, row.slot.tts_values.clone(), &row.system_bindings);
                let mut keys = Vec::with_capacity(self.items.len());
                for item in &self.items {
                    keys.push(eval_expr(&item.expr, &mut row.slot, ctx)?);
                }
                keyed_rows.push((keys, row));
            }

            let mut sort_error = None;
            keyed_rows.sort_by(|(left_keys, _), (right_keys, _)| {
                match compare_order_by_keys(&self.items, left_keys, right_keys) {
                    Ok(ordering) => ordering,
                    Err(err) => {
                        if sort_error.is_none() {
                            sort_error = Some(err);
                        }
                        std::cmp::Ordering::Equal
                    }
                }
            });
            if let Some(err) = sort_error {
                return Err(err);
            }
            self.rows = Some(keyed_rows.into_iter().map(|(_, row)| row).collect());
        }

        let rows = self.rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        }

        let idx = self.next_index;
        self.next_index += 1;
        self.current_bindings = rows[idx].system_bindings.clone();
        set_active_system_bindings(ctx, &self.current_bindings);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut rows[idx].slot))
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx).map(|row| &mut row.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn column_names(&self) -> &[String] {
        self.input.column_names()
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Sort".into()
    }
    fn explain_details(
        &self,
        indent: usize,
        analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = "  ".repeat(indent + 1);
        let sort_keys = self
            .items
            .iter()
            .map(|item| render_explain_expr(&item.expr, self.column_names()))
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!("{prefix}Sort Key: {sort_keys}"));
        if analyze {
            let memory_kb = self
                .rows
                .as_ref()
                .map(|rows| {
                    let bytes = rows.len().saturating_mul(self.plan_info.plan_width.max(1));
                    bytes.max(1024).div_ceil(1024)
                })
                .unwrap_or(1);
            lines.push(format!(
                "{prefix}Sort Method: quicksort  Memory: {memory_kb}kB"
            ));
            if self.stats.buffer_usage.shared_hit > 0
                || self.stats.buffer_usage.shared_read > 0
                || self.stats.buffer_usage.shared_written > 0
            {
                lines.push(format!(
                    "{prefix}{}",
                    crate::backend::commands::explain::format_buffer_usage(self.stats.buffer_usage)
                ));
            }
        }
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(&*self.input, indent + 1, analyze, show_costs, lines);
    }
}

impl PlanNode for LimitState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        if let Some(limit) = self.limit {
            if self.returned >= limit {
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }
        }

        while self.skipped < self.offset {
            ctx.check_for_interrupts()?;
            if self.input.exec_proc_node(ctx)?.is_none() {
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }
            self.skipped += 1;
        }

        let slot = self.input.exec_proc_node(ctx)?;
        if slot.is_some() {
            self.returned += 1;
            finish_row(&mut self.stats, start);
        } else {
            finish_eof(&mut self.stats, start, ctx);
        }
        Ok(slot)
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        self.input.current_slot()
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        self.input.current_system_bindings()
    }
    fn column_names(&self) -> &[String] {
        self.input.column_names()
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Limit".into()
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(&*self.input, indent + 1, analyze, show_costs, lines);
    }
}

impl PlanNode for LockRowsState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        loop {
            let Some(_slot) = self.input.exec_proc_node(ctx)? else {
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            };
            self.current_bindings = self.input.current_system_bindings().to_vec();
            lock_current_row_marks(ctx, &self.row_marks, &self.current_bindings)?;
            set_active_system_bindings(ctx, &self.current_bindings);
            finish_row(&mut self.stats, start);
            return Ok(self.input.current_slot());
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        self.input.current_slot()
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        self.input.column_names()
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        "LockRows".into()
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(&*self.input, indent + 1, analyze, show_costs, lines);
    }
}

impl PlanNode for ProjectionState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        let input_slot = match self.input.exec_proc_node(ctx)? {
            Some(s) => s,
            None => {
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }
        };
        let mut values = Vec::with_capacity(self.targets.len());
        let outer_values = materialize_slot_values(input_slot)?;
        let current_bindings = ctx.system_bindings.clone();
        set_outer_expr_bindings(ctx, outer_values, &current_bindings);
        clear_inner_expr_bindings(ctx);
        for target in &self.targets {
            values.push(eval_expr(&target.expr, input_slot, ctx)?.to_owned_value());
        }

        self.slot
            .store_virtual_row(values, input_slot.tid(), input_slot.table_oid);
        self.current_bindings = self.input.current_system_bindings().to_vec();
        set_active_system_bindings(ctx, &self.current_bindings);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut self.slot))
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn column_names(&self) -> &[String] {
        &self.column_names
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Projection".into()
    }
    fn explain_passthrough(&self) -> Option<&dyn PlanNode> {
        projection_is_explain_passthrough(self).then_some(self.input.as_ref())
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(&*self.input, indent + 1, analyze, show_costs, lines);
    }
}

fn projection_is_explain_passthrough(state: &ProjectionState) -> bool {
    let input_names = state.input.column_names();
    let identity_projection = state.targets.len() == input_names.len()
        && state.targets.iter().enumerate().all(|(index, target)| {
            !target.resjunk
                && target.input_resno == Some(index + 1)
                && target.name == input_names[index]
        });
    if identity_projection {
        return true;
    }
    let full_width_projection = state.targets.len() == input_names.len()
        && state.targets.iter().all(|target| !target.resjunk);
    if state.input.node_label() == "WindowAgg" && full_width_projection {
        return true;
    }
    full_width_projection
        && state
            .targets
            .iter()
            .all(|target| matches!(target.expr, Expr::Var(_)))
}

impl PlanNode for AggregateState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        if self.result_rows.is_none() {
            if self.runtimes.is_none() {
                self.runtimes = Some(
                    self.accumulators
                        .iter()
                        .map(|accum| build_aggregate_runtime(accum, ctx))
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
            let runtimes = self
                .runtimes
                .clone()
                .expect("aggregate runtimes initialized above");
            let mut groups: Vec<AggGroup> = Vec::new();

            while let Some(slot) = self.input.exec_proc_node(ctx)? {
                ctx.check_for_interrupts()?;
                let outer_values = materialize_slot_values(slot)?;
                let current_bindings = ctx.system_bindings.clone();
                set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                clear_inner_expr_bindings(ctx);
                self.key_buffer.clear();
                for expr in &self.group_by {
                    self.key_buffer.push(eval_expr(expr, slot, ctx)?);
                }

                let group_idx = groups
                    .iter()
                    .position(|g| g.key_values == self.key_buffer)
                    .unwrap_or_else(|| {
                        let accum_states = runtimes
                            .iter()
                            .zip(self.accumulators.iter())
                            .map(|(runtime, accum)| runtime.initialize_state(accum))
                            .collect();
                        groups.push(AggGroup {
                            key_values: self.key_buffer.clone(),
                            accum_states,
                            distinct_inputs: self
                                .accumulators
                                .iter()
                                .map(|accum| accum.distinct.then(HashSet::new))
                                .collect(),
                            ordered_inputs: vec![Vec::new(); self.accumulators.len()],
                        });
                        groups.len() - 1
                    });

                let group = &mut groups[group_idx];
                for (i, accum) in self.accumulators.iter().enumerate() {
                    if let Some(filter) = accum.filter.as_ref() {
                        match eval_expr(filter, slot, ctx)? {
                            Value::Bool(true) => {}
                            Value::Bool(false) | Value::Null => continue,
                            other => return Err(ExecError::NonBoolQual(other)),
                        }
                    }
                    let values = accum
                        .args
                        .iter()
                        .map(|arg| eval_expr(arg, slot, ctx))
                        .collect::<Result<Vec<_>, _>>()?;
                    if let Some(seen_inputs) = group.distinct_inputs[i].as_mut() {
                        if !seen_inputs.insert(values.clone()) {
                            continue;
                        }
                    }
                    if accum.order_by.is_empty() {
                        runtimes[i].transition(&mut group.accum_states[i], &values, ctx)?;
                    } else {
                        let sort_keys = accum
                            .order_by
                            .iter()
                            .map(|item| eval_expr(&item.expr, slot, ctx))
                            .collect::<Result<Vec<_>, _>>()?;
                        group.ordered_inputs[i].push(OrderedAggInput {
                            sort_keys,
                            arg_values: values,
                        });
                    }
                }
            }

            if groups.is_empty() && self.group_by.is_empty() {
                let accum_states = runtimes
                    .iter()
                    .zip(self.accumulators.iter())
                    .map(|(runtime, accum)| runtime.initialize_state(accum))
                    .collect();
                groups.push(AggGroup {
                    key_values: Vec::new(),
                    accum_states,
                    distinct_inputs: self
                        .accumulators
                        .iter()
                        .map(|accum| accum.distinct.then(HashSet::new))
                        .collect(),
                    ordered_inputs: vec![Vec::new(); self.accumulators.len()],
                });
            }

            for group in &mut groups {
                for (i, accum) in self.accumulators.iter().enumerate() {
                    if accum.order_by.is_empty() {
                        continue;
                    }
                    let inputs = &mut group.ordered_inputs[i];
                    let mut sort_error = None;
                    inputs.sort_by(|left, right| {
                        match compare_order_by_keys(
                            &accum.order_by,
                            &left.sort_keys,
                            &right.sort_keys,
                        ) {
                            Ok(ordering) => ordering,
                            Err(err) => {
                                if sort_error.is_none() {
                                    sort_error = Some(err);
                                }
                                std::cmp::Ordering::Equal
                            }
                        }
                    });
                    if let Some(err) = sort_error {
                        return Err(err);
                    }
                    for input in inputs.iter() {
                        runtimes[i].transition(
                            &mut group.accum_states[i],
                            &input.arg_values,
                            ctx,
                        )?;
                    }
                }
            }

            let mut result_rows = Vec::new();
            for group in &groups {
                ctx.check_for_interrupts()?;
                let mut row_values = group.key_values.clone();
                for (runtime, accum_state) in runtimes.iter().zip(group.accum_states.iter()) {
                    row_values.push(runtime.finalize(accum_state, ctx)?);
                }

                if let Some(having) = &self.having {
                    let mut having_slot = TupleSlot::virtual_row(row_values.clone());
                    ctx.system_bindings.clear();
                    set_outer_expr_bindings(ctx, row_values.clone(), &[]);
                    match eval_expr(having, &mut having_slot, ctx)? {
                        Value::Bool(true) => {}
                        Value::Bool(false) | Value::Null => continue,
                        other => return Err(ExecError::NonBoolQual(other)),
                    }
                }

                result_rows.push(MaterializedRow::new(
                    TupleSlot::virtual_row(row_values),
                    Vec::new(),
                ));
            }

            self.result_rows = Some(result_rows);
        }

        let rows = self.result_rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        }

        let idx = self.next_index;
        self.next_index += 1;
        self.current_bindings = rows[idx].system_bindings.clone();
        set_active_system_bindings(ctx, &self.current_bindings);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut rows[idx].slot))
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.result_rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx).map(|row| &mut row.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn column_names(&self) -> &[String] {
        &self.output_columns
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Aggregate".into()
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(&*self.input, indent + 1, analyze, show_costs, lines);
    }
}

impl PlanNode for WindowAggState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx);
        if self.result_rows.is_none() {
            let mut input_rows = Vec::new();
            while self.input.exec_proc_node(ctx)?.is_some() {
                ctx.check_for_interrupts()?;
                input_rows.push(self.input.materialize_current_row()?);
            }
            self.result_rows = Some(execute_window_clause(ctx, &self.clause, input_rows)?);
        }

        let rows = self.result_rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        }

        let idx = self.next_index;
        self.next_index += 1;
        self.current_bindings = rows[idx].system_bindings.clone();
        set_active_system_bindings(ctx, &self.current_bindings);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut rows[idx].slot))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.result_rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx).map(|row| &mut row.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.output_columns
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        "WindowAgg".into()
    }

    fn explain_details(
        &self,
        indent: usize,
        _analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = "  ".repeat(indent + 1);
        if !self.clause.spec.partition_by.is_empty() {
            let partition_by = self
                .clause
                .spec
                .partition_by
                .iter()
                .map(|expr| render_explain_expr(expr, self.input.column_names()))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!("{prefix}Partition By: {partition_by}"));
        }
        if !self.clause.spec.order_by.is_empty() {
            let order_by = self
                .clause
                .spec
                .order_by
                .iter()
                .map(|item| render_explain_expr(&item.expr, self.input.column_names()))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!("{prefix}Order By: {order_by}"));
        }
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(&*self.input, indent + 1, analyze, show_costs, lines);
    }
}

impl PlanNode for FunctionScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        if self.rows.is_none() {
            let mut dummy = TupleSlot::empty(0);
            self.rows = Some(
                eval_set_returning_call(&self.call, &mut dummy, ctx)?
                    .into_iter()
                    .map(|slot| MaterializedRow::new(slot, Vec::new()))
                    .collect(),
            );
        }

        let rows = self.rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        }

        let idx = self.next_index;
        self.next_index += 1;
        self.current_bindings = rows[idx].system_bindings.clone();
        set_active_system_bindings(ctx, &self.current_bindings);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut rows[idx].slot))
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx).map(|row| &mut row.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn column_names(&self) -> &[String] {
        &self.output_columns
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        format!("Function Scan on {}", set_returning_call_label(&self.call))
    }
    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

impl PlanNode for SubqueryScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        let slot = self.input.exec_proc_node(ctx)?;
        if slot.is_some() {
            finish_row(&mut self.stats, start);
        } else {
            finish_eof(&mut self.stats, start, ctx);
        }
        Ok(slot)
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        self.input.current_slot()
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        self.input.current_system_bindings()
    }

    fn column_names(&self) -> &[String] {
        &self.output_columns
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        "Subquery Scan".into()
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(&*self.input, indent + 1, analyze, show_costs, lines);
    }
}

impl PlanNode for ValuesState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        if self.result_rows.is_none() {
            let mut dummy = TupleSlot::empty(0);
            let rows = self
                .rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|expr| eval_expr(expr, &mut dummy, ctx))
                        .collect::<Result<Vec<_>, ExecError>>()
                        .map(|slot_values| {
                            MaterializedRow::new(TupleSlot::virtual_row(slot_values), Vec::new())
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.result_rows = Some(rows);
        }

        let rows = self.result_rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        }
        let idx = self.next_index;
        self.next_index += 1;
        self.current_bindings = rows[idx].system_bindings.clone();
        set_active_system_bindings(ctx, &self.current_bindings);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut rows[idx].slot))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.result_rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx).map(|row| &mut row.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.output_columns
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Values Scan".into()
    }
    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

impl PlanNode for WorkTableScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx);
        let Some(worktable) = ctx.recursive_worktables.get(&self.worktable_id).cloned() else {
            return Err(ExecError::DetailedError {
                message: "worktable scan executed without an active recursive union".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            });
        };
        let Some(row) = worktable.borrow().rows.get(self.next_index).cloned() else {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        };
        self.next_index += 1;
        load_materialized_row(&mut self.slot, &row, &mut self.current_bindings, ctx);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut self.slot))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.output_columns
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "WorkTable Scan".into()
    }
    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

impl PlanNode for CteScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx);
        let table = ctx
            .cte_tables
            .entry(self.cte_id)
            .or_insert_with(|| Rc::new(RefCell::new(Default::default())))
            .clone();
        loop {
            ctx.check_for_interrupts()?;
            if let Some(row) = table.borrow().rows.get(self.next_index).cloned() {
                self.next_index += 1;
                load_materialized_row(&mut self.slot, &row, &mut self.current_bindings, ctx);
                finish_row(&mut self.stats, start);
                return Ok(Some(&mut self.slot));
            }
            if table.borrow().eof {
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }
            let producer = ctx
                .cte_producers
                .entry(self.cte_id)
                .or_insert_with(|| Rc::new(RefCell::new(executor_start(self.cte_plan.clone()))))
                .clone();
            let mut producer_state = producer.borrow_mut();
            if producer_state.exec_proc_node(ctx)?.is_some() {
                let row = producer_state.materialize_current_row()?;
                table.borrow_mut().rows.push(row);
            } else {
                table.borrow_mut().eof = true;
            }
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.output_columns
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "CTE Scan".into()
    }
    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

impl PlanNode for RecursiveUnionState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx);
        if self.distinct && !self.distinct_hashable {
            finish_eof(&mut self.stats, start, ctx);
            return Err(ExecError::DetailedError {
                message: "could not implement recursive UNION".into(),
                detail: Some("All column datatypes must be hashable.".into()),
                hint: None,
                sqlstate: "0A000",
            });
        }
        ctx.recursive_worktables
            .insert(self.worktable_id, self.worktable.clone());

        loop {
            ctx.check_for_interrupts()?;
            if !self.anchor_done {
                if self.anchor.exec_proc_node(ctx)?.is_some() {
                    let mut row = self.anchor.materialize_current_row()?;
                    if self.distinct {
                        let signature = row.slot.values()?.to_vec();
                        if !self.seen_rows.insert(signature) {
                            continue;
                        }
                    }
                    self.worktable.borrow_mut().rows.push(row.clone());
                    load_materialized_row(&mut self.slot, &row, &mut self.current_bindings, ctx);
                    finish_row(&mut self.stats, start);
                    return Ok(Some(&mut self.slot));
                } else {
                    self.anchor_done = true;
                    self.recursive_state = Some(executor_start(self.recursive_plan.clone()));
                    continue;
                }
            }

            let recursive_state = self
                .recursive_state
                .get_or_insert_with(|| executor_start(self.recursive_plan.clone()));
            if recursive_state.exec_proc_node(ctx)?.is_some() {
                let mut row = recursive_state.materialize_current_row()?;
                if self.distinct {
                    let signature = row.slot.values()?.to_vec();
                    if !self.seen_rows.insert(signature) {
                        continue;
                    }
                }
                self.intermediate_rows.push(row.clone());
                load_materialized_row(&mut self.slot, &row, &mut self.current_bindings, ctx);
                finish_row(&mut self.stats, start);
                return Ok(Some(&mut self.slot));
            } else {
                if self.intermediate_rows.is_empty() {
                    ctx.recursive_worktables.remove(&self.worktable_id);
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                }
                if !self.recursive_references_worktable {
                    ctx.recursive_worktables.remove(&self.worktable_id);
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                }
                self.worktable.borrow_mut().rows = std::mem::take(&mut self.intermediate_rows);
                self.recursive_state = Some(executor_start(self.recursive_plan.clone()));
            }
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.output_columns
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Recursive Union".into()
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(&*self.anchor, indent + 1, analyze, show_costs, lines);
        if let Some(recursive_state) = &self.recursive_state {
            format_explain_lines_with_costs(
                &**recursive_state,
                indent + 1,
                analyze,
                show_costs,
                lines,
            );
        } else {
            let recursive_state = executor_start(self.recursive_plan.clone());
            format_explain_lines_with_costs(
                &*recursive_state,
                indent + 1,
                analyze,
                show_costs,
                lines,
            );
        }
    }
}

impl PlanNode for SetOpState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx);

        if self.result_rows.is_none() {
            self.result_rows = Some(set_op_result_rows(self.op, &mut self.children, ctx)?);
        }

        let Some(rows) = self.result_rows.as_ref() else {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        };
        if self.next_index >= rows.len() {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        }
        let row = rows[self.next_index].clone();
        self.next_index += 1;
        load_materialized_row(&mut self.slot, &row, &mut self.current_bindings, ctx);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut self.slot))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.output_columns
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        match self.op {
            crate::include::nodes::parsenodes::SetOperator::Union { all: true } => {
                "SetOp Union All".into()
            }
            crate::include::nodes::parsenodes::SetOperator::Union { all: false } => {
                "SetOp Union".into()
            }
            crate::include::nodes::parsenodes::SetOperator::Intersect { all: true } => {
                "SetOp Intersect All".into()
            }
            crate::include::nodes::parsenodes::SetOperator::Intersect { all: false } => {
                "SetOp Intersect".into()
            }
            crate::include::nodes::parsenodes::SetOperator::Except { all: true } => {
                "SetOp Except All".into()
            }
            crate::include::nodes::parsenodes::SetOperator::Except { all: false } => {
                "SetOp Except".into()
            }
        }
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        for child in &self.children {
            format_explain_lines_with_costs(child.as_ref(), indent, analyze, show_costs, lines);
        }
    }
}

impl PlanNode for ProjectSetState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        loop {
            ctx.check_for_interrupts()?;
            if self.current_input.is_none() || self.next_index >= self.current_row_count {
                let Some(input_slot) = self.input.exec_proc_node(ctx)? else {
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                };
                let mut values = input_slot.values()?.to_vec();
                Value::materialize_all(&mut values);
                let mut materialized = TupleSlot::virtual_row(values);
                let mut srf_rows = Vec::new();
                let mut max_rows = 0usize;
                for target in &self.targets {
                    if let crate::include::nodes::primnodes::ProjectSetTarget::Set {
                        call,
                        column_index,
                        ..
                    } = target
                    {
                        set_outer_expr_bindings(
                            ctx,
                            materialized.tts_values.clone(),
                            self.input.current_system_bindings(),
                        );
                        let rows = eval_project_set_returning_call(
                            call,
                            *column_index,
                            &mut materialized,
                            ctx,
                        )?;
                        max_rows = max_rows.max(rows.len());
                        srf_rows.push(rows);
                    }
                }

                if max_rows == 0 {
                    self.current_input = None;
                    self.current_srf_rows.clear();
                    self.current_row_count = 0;
                    self.next_index = 0;
                    continue;
                }

                self.current_input = Some(MaterializedRow::new(
                    materialized,
                    self.input.current_system_bindings().to_vec(),
                ));
                self.current_srf_rows = srf_rows;
                self.current_row_count = max_rows;
                self.next_index = 0;
            }

            let input_slot = self.current_input.as_mut().unwrap();
            let row_idx = self.next_index;
            self.next_index += 1;

            let mut values = Vec::with_capacity(self.targets.len());
            let mut srf_idx = 0usize;
            for target in &self.targets {
                match target {
                    crate::include::nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
                        set_active_system_bindings(ctx, &input_slot.system_bindings);
                        set_outer_expr_bindings(
                            ctx,
                            input_slot.slot.tts_values.clone(),
                            &input_slot.system_bindings,
                        );
                        values.push(
                            eval_expr(&entry.expr, &mut input_slot.slot, ctx)?.to_owned_value(),
                        );
                    }
                    crate::include::nodes::primnodes::ProjectSetTarget::Set { .. } => {
                        values.push(
                            self.current_srf_rows[srf_idx]
                                .get(row_idx)
                                .cloned()
                                .unwrap_or(Value::Null),
                        );
                        srf_idx += 1;
                    }
                }
            }

            self.slot
                .store_virtual_row(values, input_slot.slot.tid(), input_slot.slot.table_oid);
            self.current_bindings = input_slot.system_bindings.clone();
            set_active_system_bindings(ctx, &self.current_bindings);

            if self.next_index >= self.current_row_count {
                self.current_input = None;
                self.current_srf_rows.clear();
                self.current_row_count = 0;
                self.next_index = 0;
            }

            finish_row(&mut self.stats, start);
            return Ok(Some(&mut self.slot));
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.output_columns
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "ProjectSet".into()
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(&*self.input, indent + 1, analyze, show_costs, lines);
    }
}

impl TupleSlot {
    pub fn values(&mut self) -> Result<&[Value], ExecError> {
        let ncols = self.ncols();
        self.slot_getsomeattrs(ncols)
    }

    pub fn slot_getsomeattrs(&mut self, natts: usize) -> Result<&[Value], ExecError> {
        if self.tts_nvalid >= natts {
            return Ok(&self.tts_values[..natts]);
        }
        match &self.kind {
            SlotKind::Virtual => Ok(&self.tts_values[..natts]),
            SlotKind::BufferHeapTuple {
                desc,
                attr_descs,
                tuple_ptr,
                tuple_len,
                ..
            } => {
                let (ptr, len) = (*tuple_ptr, *tuple_len);
                let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
                if self.toast.is_some() {
                    let raw = crate::include::access::htup::deform_raw(bytes, attr_descs)?;
                    self.tts_values.clear();
                    for (index, column) in desc.columns.iter().enumerate() {
                        if let Some(datum) = raw.get(index) {
                            self.tts_values.push(decode_value_with_toast(
                                column,
                                *datum,
                                self.toast.as_ref(),
                            )?);
                        } else {
                            self.tts_values.push(missing_column_value(column));
                        }
                    }
                    self.tts_nvalid = self.tts_values.len();
                } else {
                    let decoder = self
                        .decoder
                        .as_ref()
                        .expect("BufferHeapTuple requires decoder");
                    decoder.decode_range(
                        bytes,
                        &mut self.tts_values,
                        self.tts_nvalid,
                        natts,
                        &mut self.decode_offset,
                    )?;
                    self.tts_nvalid = natts;
                }
                Ok(&self.tts_values[..natts])
            }
            SlotKind::HeapTuple {
                desc,
                attr_descs,
                tuple,
                ..
            } => {
                let raw = tuple.deform(attr_descs)?;
                self.tts_values.clear();
                for (index, column) in desc.columns.iter().enumerate() {
                    if let Some(datum) = raw.get(index) {
                        self.tts_values.push(decode_value_with_toast(
                            column,
                            *datum,
                            self.toast.as_ref(),
                        )?);
                    } else {
                        self.tts_values.push(missing_column_value(column));
                    }
                }
                self.tts_nvalid = self.tts_values.len();
                Ok(&self.tts_values[..natts])
            }
            SlotKind::Empty => {
                panic!("cannot get attrs from empty slot")
            }
        }
    }

    pub fn get_attr(&mut self, index: usize) -> Result<&Value, ExecError> {
        self.slot_getsomeattrs(index + 1)?;
        Ok(&self.tts_values[index])
    }

    pub fn into_values(mut self) -> Result<Vec<Value>, ExecError> {
        self.values()?;
        Value::materialize_all(&mut self.tts_values);
        Ok(self.tts_values)
    }
}
