use super::{AccumState, AggGroup, ExecError, ExecutorContext, OrderedAggInput, executor_start};
use crate::backend::access::heap::heapam::{
    heap_fetch_visible_with_txns, heap_scan_begin_visible, heap_scan_end,
    heap_scan_page_next_tuple, heap_scan_prepare_next_page,
};
use crate::backend::access::index::indexam;
use crate::backend::commands::explain::format_explain_lines_with_costs;
use crate::backend::executor::exec_expr::{compare_order_by_keys, eval_expr};
use crate::backend::executor::pg_regex::explain_similar_pattern;
use crate::backend::executor::srf::{
    eval_project_set_returning_call, eval_set_returning_call, set_returning_call_label,
};
use crate::backend::executor::value_io::{decode_value_with_toast, missing_column_value};
use crate::backend::executor::window::execute_window_clause;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::utils::time::instant::Instant;
use crate::include::catalog::PG_LARGEOBJECT_METADATA_RELATION_OID;
use crate::include::catalog::builtin_aggregate_function_for_proc_oid;
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::{
    AggregateState, AppendState, CteScanState, FilterState, FunctionScanState, IndexScanState,
    LimitState, MaterializedRow, NestedLoopJoinState, NodeExecStats, OrderByState, PlanNode,
    PlanState, ProjectSetState, ProjectionState, RecursiveUnionState, ResultState, SeqScanState,
    SetOpState, SlotKind, SubqueryScanState, SystemVarBinding, ToastRelationRef, TupleSlot,
    ValuesState, WindowAggState, WorkTableScanState,
};
use crate::include::nodes::primnodes::{
    Expr, INDEX_VAR, INNER_VAR, JoinType, OUTER_VAR, Var, attrno_index,
};
use std::cell::RefCell;
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

fn materialize_cte_row(slot: &mut TupleSlot) -> Result<MaterializedRow, ExecError> {
    let mut values = slot.values()?.to_vec();
    Value::materialize_all(&mut values);
    Ok(MaterializedRow::new(
        TupleSlot::virtual_row(values),
        Vec::new(),
    ))
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
        while let Some(slot) = child.exec_proc_node(ctx)? {
            let mut values = slot.values()?.to_vec();
            Value::materialize_all(&mut values);
            let row = MaterializedRow::new(TupleSlot::virtual_row(values.clone()), Vec::new());
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
            self.slot.kind = SlotKind::Virtual;
            self.slot.virtual_tid = None;
            self.slot.tts_values.clear();
            self.slot.tts_nvalid = 0;
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
                let mut values = slot.values()?.to_vec();
                Value::materialize_all(&mut values);
                self.slot.kind = SlotKind::Virtual;
                self.slot.virtual_tid = None;
                self.slot.tts_nvalid = values.len();
                self.slot.tts_values = values;
                self.slot.decode_offset = 0;
                self.current_bindings = self.children[self.current_child]
                    .current_system_bindings()
                    .first()
                    .map(|binding| {
                        vec![SystemVarBinding {
                            varno: self.source_id,
                            table_oid: binding.table_oid,
                        }]
                    })
                    .unwrap_or_default();
                self.slot.table_oid = self
                    .current_bindings
                    .first()
                    .map(|binding| binding.table_oid);
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
                self.slot.kind = SlotKind::Virtual;
                self.slot.virtual_tid = None;
                self.slot.tts_values = values;
                self.slot.tts_nvalid = self.slot.tts_values.len();
                self.slot.decode_offset = 0;
                self.slot.toast = None;
                self.slot.table_oid = Some(self.relation_oid);
                self.current_bindings = vec![SystemVarBinding {
                    varno: self.source_id,
                    table_oid: self.relation_oid,
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
            self.slot.kind = SlotKind::Virtual;
            self.slot.virtual_tid = None;
            self.slot.tts_values = values;
            self.slot.tts_nvalid = self.slot.tts_values.len();
            self.slot.decode_offset = 0;
            self.slot.toast = None;
            self.slot.table_oid = Some(self.relation_oid);
            self.current_bindings = vec![SystemVarBinding {
                varno: self.source_id,
                table_oid: self.relation_oid,
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
        if self.scan.is_none() {
            let begin = crate::include::access::amapi::IndexBeginScanContext {
                pool: ctx.pool.clone(),
                client_id: ctx.client_id,
                snapshot: ctx.snapshot.clone(),
                heap_relation: self.rel,
                index_relation: self.index_rel,
                index_desc: (*self.index_desc).clone(),
                index_meta: self.index_meta.clone(),
                key_data: self.keys.clone(),
                order_by_data: self.order_by_keys.clone(),
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

        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };

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
            "Index Scan using rel {} on rel {}",
            self.index_rel.rel_number, self.rel.rel_number
        )
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

pub(crate) fn render_explain_expr(expr: &Expr, column_names: &[String]) -> String {
    format!("({})", render_explain_expr_inner(expr, column_names))
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
    match expr {
        Expr::Var(var) => {
            render_explain_var_name(var, column_names).unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Const(value) => render_explain_const(value),
        Expr::Cast(inner, ty) => render_explain_cast(inner, *ty, column_names),
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
                    render_explain_expr_inner(left, column_names),
                    op_text,
                    render_explain_expr_inner(right, column_names)
                )
            }
            _ => format!("{expr:?}"),
        },
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            crate::include::nodes::primnodes::BoolExprType::And => {
                let rendered = bool_expr
                    .args
                    .iter()
                    .map(|arg| render_explain_expr_inner(arg, column_names))
                    .collect::<Vec<_>>();
                format!("({})", rendered.join(" AND "))
            }
            crate::include::nodes::primnodes::BoolExprType::Or => {
                let rendered = bool_expr
                    .args
                    .iter()
                    .map(|arg| render_explain_expr_inner(arg, column_names))
                    .collect::<Vec<_>>();
                format!("({})", rendered.join(" OR "))
            }
            crate::include::nodes::primnodes::BoolExprType::Not => {
                let Some(inner) = bool_expr.args.first() else {
                    return format!("{expr:?}");
                };
                format!("NOT {}", render_explain_expr_inner(inner, column_names))
            }
        },
        Expr::Coalesce(left, right) => format!(
            "COALESCE({}, {})",
            render_explain_expr_inner(left, column_names),
            render_explain_expr_inner(right, column_names)
        ),
        Expr::IsNull(inner) => {
            format!("{} IS NULL", render_explain_expr_inner(inner, column_names))
        }
        Expr::IsNotNull(inner) => {
            format!(
                "{} IS NOT NULL",
                render_explain_expr_inner(inner, column_names)
            )
        }
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => render_similar_explain_expr(expr, pattern, escape.as_deref(), *negated, |expr| {
            render_explain_expr_inner(expr, column_names)
        }),
        other => format!("{other:?}"),
    }
}

fn render_explain_join_expr_inner(
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
        Value::Int16(v) => format!("{v}::smallint"),
        Value::Int32(v) => format!("{v}::integer"),
        Value::Int64(v) => format!("{v}::bigint"),
        Value::Bool(v) => format!("{}::boolean", if *v { "true" } else { "false" }),
        Value::Null => "NULL".to_string(),
        other => format!("{other:?}"),
    }
}

fn render_explain_cast(expr: &Expr, ty: SqlType, column_names: &[String]) -> String {
    if let Expr::Const(value) = expr {
        return format!(
            "{}::{}",
            render_explain_literal(value),
            render_explain_sql_type_name(ty)
        );
    }
    let inner = render_explain_expr_inner(expr, column_names);
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
                    if eval_qual_list(&self.qual, &mut self.slot, ctx)? {
                        finish_row(&mut self.stats, start);
                        return Ok(Some(&mut self.slot));
                    }
                }
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
        &self.combined_names
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
                if eval_qual_list(&state.qual, &mut state.slot, ctx)? {
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

            keyed_rows.sort_by(|(left_keys, _), (right_keys, _)| {
                compare_order_by_keys(&self.items, left_keys, right_keys)
            });
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

        let nvalid = values.len();
        self.slot.tts_values = values;
        self.slot.tts_nvalid = nvalid;
        self.slot.kind = SlotKind::Virtual;
        self.slot.virtual_tid = None;
        self.slot.decode_offset = 0;
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
    state.targets.len() == input_names.len()
        && state.targets.iter().enumerate().all(|(index, target)| {
            !target.resjunk
                && target.input_resno == Some(index + 1)
                && target.name == input_names[index]
        })
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
                        let accum_states = self
                            .accumulators
                            .iter()
                            .map(|a| {
                                let func = builtin_aggregate_function_for_proc_oid(a.aggfnoid)
                                    .unwrap_or_else(|| {
                                        panic!(
                                            "aggregate {:?} lacks builtin implementation mapping",
                                            a.aggfnoid
                                        )
                                    });
                                AccumState::new(func, a.distinct, a.sql_type)
                            })
                            .collect();
                        groups.push(AggGroup {
                            key_values: self.key_buffer.clone(),
                            accum_states,
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
                    if accum.order_by.is_empty() {
                        (self.trans_fns[i])(&mut group.accum_states[i], &values)?;
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
                let accum_states = self
                    .accumulators
                    .iter()
                    .map(|a| {
                        let func = builtin_aggregate_function_for_proc_oid(a.aggfnoid)
                            .unwrap_or_else(|| {
                                panic!(
                                    "aggregate {:?} lacks builtin implementation mapping",
                                    a.aggfnoid
                                )
                            });
                        AccumState::new(func, a.distinct, a.sql_type)
                    })
                    .collect();
                groups.push(AggGroup {
                    key_values: Vec::new(),
                    accum_states,
                    ordered_inputs: vec![Vec::new(); self.accumulators.len()],
                });
            }

            for group in &mut groups {
                for (i, accum) in self.accumulators.iter().enumerate() {
                    if accum.order_by.is_empty() {
                        continue;
                    }
                    let inputs = &mut group.ordered_inputs[i];
                    inputs.sort_by(|left, right| {
                        compare_order_by_keys(&accum.order_by, &left.sort_keys, &right.sort_keys)
                    });
                    for input in inputs.iter() {
                        (self.trans_fns[i])(&mut group.accum_states[i], &input.arg_values)?;
                    }
                }
            }

            let mut result_rows = Vec::new();
            for group in &groups {
                ctx.check_for_interrupts()?;
                let mut row_values = group.key_values.clone();
                for accum_state in &group.accum_states {
                    row_values.push(accum_state.finalize());
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
            match producer.borrow_mut().exec_proc_node(ctx)? {
                Some(slot) => {
                    let row = materialize_cte_row(slot)?;
                    table.borrow_mut().rows.push(row);
                }
                None => {
                    table.borrow_mut().eof = true;
                }
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
                match self.anchor.exec_proc_node(ctx)? {
                    Some(slot) => {
                        let mut row = materialize_cte_row(slot)?;
                        if self.distinct {
                            let signature = row.slot.values()?.to_vec();
                            if !self.seen_rows.insert(signature) {
                                continue;
                            }
                        }
                        self.worktable.borrow_mut().rows.push(row.clone());
                        load_materialized_row(
                            &mut self.slot,
                            &row,
                            &mut self.current_bindings,
                            ctx,
                        );
                        finish_row(&mut self.stats, start);
                        return Ok(Some(&mut self.slot));
                    }
                    None => {
                        self.anchor_done = true;
                        self.recursive_state = Some(executor_start(self.recursive_plan.clone()));
                        continue;
                    }
                }
            }

            let recursive_state = self
                .recursive_state
                .get_or_insert_with(|| executor_start(self.recursive_plan.clone()));
            match recursive_state.exec_proc_node(ctx)? {
                Some(slot) => {
                    let mut row = materialize_cte_row(slot)?;
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
                }
                None => {
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

            self.slot.kind = SlotKind::Virtual;
            self.slot.virtual_tid = None;
            self.slot.tts_values = values;
            self.slot.tts_nvalid = self.slot.tts_values.len();
            self.slot.decode_offset = 0;
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
