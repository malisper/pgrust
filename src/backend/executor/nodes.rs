use super::{AccumState, AggGroup, ExecError, ExecutorContext};
use crate::backend::access::heap::heapam::{
    heap_fetch_visible_with_txns, heap_scan_begin_visible, heap_scan_end,
    heap_scan_page_next_tuple, heap_scan_prepare_next_page,
};
use crate::backend::access::index::indexam;
use crate::backend::commands::explain::format_explain_lines;
use crate::backend::executor::exec_expr::{compare_order_by_keys, eval_expr};
use crate::backend::executor::srf::{
    eval_scalar_set_returning_call, eval_set_returning_call, set_returning_call_label,
};
use crate::backend::executor::value_io::{decode_value_with_toast, missing_column_value};
use crate::include::catalog::builtin_aggregate_function_for_proc_oid;
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::{
    AggregateState, AppendState, FilterState, FunctionScanState, IndexScanState, LimitState,
    NestedLoopJoinState, NodeExecStats, OrderByState, PlanNode, ProjectSetState, ProjectionState,
    ResultState, SeqScanState, SlotKind, ToastRelationRef, TupleSlot, ValuesState,
};
use crate::include::nodes::primnodes::{Expr, JoinType};

use std::time::Instant;

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

impl PlanNode for ResultState {
    fn exec_proc_node<'a>(
        &'a mut self,
        _ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.emitted {
            Ok(None)
        } else {
            self.emitted = true;
            self.slot.kind = SlotKind::Virtual;
            self.slot.tts_values.clear();
            self.slot.tts_nvalid = 0;
            Ok(Some(&mut self.slot))
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
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
    fn explain_children(&self, _indent: usize, _analyze: bool, _lines: &mut Vec<String>) {}
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
        while self.current_child < self.children.len() {
            if let Some(slot) = self.children[self.current_child].exec_proc_node(ctx)? {
                let mut values = slot.values()?.to_vec();
                Value::materialize_all(&mut values);
                self.slot.kind = SlotKind::Virtual;
                self.slot.tts_nvalid = values.len();
                self.slot.tts_values = values;
                self.slot.decode_offset = 0;
                if let Some(start) = start {
                    self.stats.loops += 1;
                    self.stats.rows += 1;
                    self.stats.total_time += start.elapsed();
                }
                return Ok(Some(&mut self.slot));
            }
            self.current_child += 1;
        }
        if let Some(start) = start {
            self.stats.loops += 1;
            self.stats.total_time += start.elapsed();
        }
        Ok(None)
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
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
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        for child in &self.children {
            format_explain_lines(child.as_ref(), indent + 1, analyze, lines);
        }
    }
}

impl PlanNode for SeqScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.scan.is_none() {
            self.scan = Some(heap_scan_begin_visible(
                &ctx.pool,
                ctx.client_id,
                self.rel,
                ctx.snapshot.clone(),
            )?);
        }

        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };

        loop {
            ctx.check_for_interrupts()?;
            let scan = self.scan.as_mut().unwrap();
            if scan.has_page_tuples() {
                let buffer_id = scan.pinned_buffer_id().expect("buffer must be pinned");
                let page = unsafe { ctx.pool.page_unlocked(buffer_id) }
                    .expect("pinned buffer must be valid");

                if let Some((_tid, tuple_bytes)) = heap_scan_page_next_tuple(page, scan) {
                    let raw_ptr = tuple_bytes.as_ptr();
                    let raw_len = tuple_bytes.len();
                    let pin = scan.pinned_buffer_rc().expect("buffer must be pinned");

                    self.slot.kind = SlotKind::BufferHeapTuple {
                        desc: self.desc.clone(),
                        attr_descs: self.attr_descs.clone(),
                        tuple_ptr: raw_ptr,
                        tuple_len: raw_len,
                        pin,
                    };
                    self.slot.toast = slot_toast_context(self.toast_relation, ctx);
                    self.slot.tts_nvalid = 0;
                    self.slot.tts_values.clear();
                    self.slot.decode_offset = 0;

                    if let Some(qual) = &self.qual {
                        if !qual(&mut self.slot, ctx)? {
                            continue;
                        }
                    }

                    if let Some(s) = start {
                        self.stats.loops += 1;
                        self.stats.total_time += s.elapsed();
                        self.stats.rows += 1;
                    }
                    return Ok(Some(&mut self.slot));
                }
            }

            let next: Result<Option<usize>, ExecError> =
                heap_scan_prepare_next_page(&*ctx.pool, ctx.client_id, &ctx.txns, scan);
            if next?.is_none() {
                heap_scan_end::<ExecError>(&*ctx.pool, ctx.client_id, scan)?;
                if let Some(s) = start {
                    self.stats.loops += 1;
                    self.stats.total_time += s.elapsed();
                }
                return Ok(None);
            }
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
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
    fn explain_children(&self, indent: usize, _analyze: bool, lines: &mut Vec<String>) {
        if let Some(qual_expr) = &self.qual_expr {
            let prefix = "  ".repeat(indent + 1);
            lines.push(format!(
                "{prefix}Filter: {}",
                render_explain_expr(qual_expr, &self.column_names)
            ));
        }
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
                index_desc: (*self.desc).clone(),
                index_meta: self.index_meta.clone(),
                key_data: self.keys.clone(),
                direction: self.direction,
            };
            self.scan = Some(
                indexam::index_beginscan(&begin, self.am_oid).map_err(|err| {
                    ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                        expected: "index access method begin scan",
                        actual: format!("{err:?}"),
                    })
                })?,
            );
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
                if let Some(s) = start {
                    self.stats.loops += 1;
                    self.stats.total_time += s.elapsed();
                }
                return Ok(None);
            }

            let tid = self
                .scan
                .as_ref()
                .and_then(|scan| scan.xs_heaptid)
                .expect("index scan tuple must set heap tid");
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

            if let Some(s) = start {
                self.stats.loops += 1;
                self.stats.total_time += s.elapsed();
                self.stats.rows += 1;
            }
            return Ok(Some(&mut self.slot));
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
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
    fn explain_children(&self, _indent: usize, _analyze: bool, _lines: &mut Vec<String>) {}
}

fn render_explain_expr(expr: &Expr, column_names: &[String]) -> String {
    format!("({})", render_explain_expr_inner(expr, column_names))
}

fn render_explain_expr_inner(expr: &Expr, column_names: &[String]) -> String {
    match expr {
        Expr::Column(index) => column_names
            .get(*index)
            .cloned()
            .unwrap_or_else(|| format!("column{}", index + 1)),
        Expr::Const(value) => render_explain_const(value),
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
        other => format!("{other:?}"),
    }
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
        loop {
            ctx.check_for_interrupts()?;
            let slot = match self.input.exec_proc_node(ctx)? {
                Some(s) => s,
                None => {
                    if let Some(s) = start {
                        self.stats.loops += 1;
                        self.stats.total_time += s.elapsed();
                    }
                    return Ok(None);
                }
            };

            if (self.compiled_predicate)(slot, ctx)? {
                if let Some(s) = start {
                    self.stats.loops += 1;
                    self.stats.total_time += s.elapsed();
                    self.stats.rows += 1;
                }
                return Ok(self.input.current_slot());
            }
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        self.input.current_slot()
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
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        format_explain_lines(&*self.input, indent, analyze, lines);
    }
}

impl PlanNode for NestedLoopJoinState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.right_plan.is_some() {
            return exec_lateral_join(self, ctx);
        }
        if matches!(self.kind, JoinType::Cross) && self.cross_right_outer {
            return exec_cross_join(self, ctx);
        }

        if self.right_rows.is_none() {
            let mut rows = Vec::new();
            while let Some(slot) = self.right.exec_proc_node(ctx)? {
                ctx.check_for_interrupts()?;
                let mut values = slot.values()?.iter().cloned().collect::<Vec<_>>();
                Value::materialize_all(&mut values);
                rows.push(TupleSlot::virtual_row(values));
            }
            self.right_matched = Some(vec![false; rows.len()]);
            self.right_rows = Some(rows);
        }

        loop {
            ctx.check_for_interrupts()?;
            if self.current_left.is_none() {
                match self.left.exec_proc_node(ctx)? {
                    Some(slot) => {
                        let mut values = slot.values()?.iter().cloned().collect::<Vec<_>>();
                        Value::materialize_all(&mut values);
                        self.current_left = Some(TupleSlot::virtual_row(values));
                        self.current_left_matched = false;
                        self.right_index = 0;
                    }
                    None => {
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
                                combined_values.extend(right_rows[ri].tts_values.iter().cloned());
                                self.slot.tts_values = combined_values;
                                self.slot.tts_nvalid = self.left_width + self.right_width;
                                self.slot.kind = SlotKind::Virtual;
                                self.slot.decode_offset = 0;
                                return Ok(Some(&mut self.slot));
                            }
                        }
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
                let mut combined_values: Vec<Value> = left.tts_values.clone();
                combined_values.extend(right.tts_values.iter().cloned());
                let nvalid = combined_values.len();
                self.slot.tts_values = combined_values;
                self.slot.tts_nvalid = nvalid;
                self.slot.kind = SlotKind::Virtual;
                self.slot.decode_offset = 0;

                match eval_expr(&self.on, &mut self.slot, ctx)? {
                    Value::Bool(true) => {
                        self.current_left_matched = true;
                        right_matched[ri] = true;
                        return Ok(Some(&mut self.slot));
                    }
                    Value::Bool(false) | Value::Null => {}
                    other => return Err(ExecError::NonBoolQual(other)),
                }
            }

            if !self.current_left_matched && matches!(self.kind, JoinType::Left | JoinType::Full) {
                let left = self.current_left.as_ref().unwrap();
                let mut combined_values: Vec<Value> = left.tts_values.clone();
                combined_values.extend(std::iter::repeat_n(Value::Null, self.right_width));
                self.slot.tts_values = combined_values;
                self.slot.tts_nvalid = self.left_width + self.right_width;
                self.slot.kind = SlotKind::Virtual;
                self.slot.decode_offset = 0;
                self.current_left = None;
                return Ok(Some(&mut self.slot));
            }

            self.current_left = None;
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
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
        "Nested Loop".into()
    }
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        format_explain_lines(&*self.left, indent, analyze, lines);
        format_explain_lines(&*self.right, indent, analyze, lines);
    }
}

fn exec_lateral_join<'a>(
    state: &'a mut NestedLoopJoinState,
    ctx: &mut ExecutorContext,
) -> Result<Option<&'a mut TupleSlot>, ExecError> {
    if matches!(state.kind, JoinType::Right | JoinType::Full) {
        return Err(ExecError::DetailedError {
            message: "unsupported lateral join type".into(),
            detail: Some(
                "outer-dependent right-hand joins are only implemented for INNER, CROSS, and LEFT joins".into(),
            ),
            hint: None,
            sqlstate: "0A000",
        });
    }

    loop {
        ctx.check_for_interrupts()?;
        if state.current_left.is_none() {
            match state.left.exec_proc_node(ctx)? {
                Some(slot) => {
                    let mut values = slot.values()?.iter().cloned().collect::<Vec<_>>();
                    Value::materialize_all(&mut values);
                    state.current_left = Some(TupleSlot::virtual_row(values.clone()));
                    state.current_left_matched = false;
                    ctx.outer_rows.insert(0, values);
                    state.right = super::executor_start(
                        state
                            .right_plan
                            .as_ref()
                            .expect("lateral right plan")
                            .clone(),
                    );
                }
                None => return Ok(None),
            }
        }

        while let Some(slot) = state.right.exec_proc_node(ctx)? {
            ctx.check_for_interrupts()?;
            let mut values = slot.values()?.iter().cloned().collect::<Vec<_>>();
            Value::materialize_all(&mut values);
            let left = state.current_left.as_ref().unwrap();
            let mut combined_values: Vec<Value> = left.tts_values.clone();
            combined_values.extend(values);
            let nvalid = combined_values.len();
            state.slot.tts_values = combined_values;
            state.slot.tts_nvalid = nvalid;
            state.slot.kind = SlotKind::Virtual;
            state.slot.decode_offset = 0;

            match eval_expr(&state.on, &mut state.slot, ctx)? {
                Value::Bool(true) => {
                    state.current_left_matched = true;
                    return Ok(Some(&mut state.slot));
                }
                Value::Bool(false) | Value::Null => {}
                other => return Err(ExecError::NonBoolQual(other)),
            }
        }

        ctx.outer_rows.remove(0);
        if !state.current_left_matched && matches!(state.kind, JoinType::Left) {
            let left = state.current_left.as_ref().unwrap();
            let mut combined_values: Vec<Value> = left.tts_values.clone();
            combined_values.extend(std::iter::repeat_n(Value::Null, state.right_width));
            state.slot.tts_values = combined_values;
            state.slot.tts_nvalid = state.left_width + state.right_width;
            state.slot.kind = SlotKind::Virtual;
            state.slot.decode_offset = 0;
            state.current_left = None;
            return Ok(Some(&mut state.slot));
        }

        state.current_left = None;
    }
}

fn exec_cross_join<'a>(
    state: &'a mut NestedLoopJoinState,
    ctx: &mut ExecutorContext,
) -> Result<Option<&'a mut TupleSlot>, ExecError> {
    if state.left_rows.is_none() {
        let mut rows = Vec::new();
        while let Some(slot) = state.left.exec_proc_node(ctx)? {
            ctx.check_for_interrupts()?;
            let mut values = slot.values()?.iter().cloned().collect::<Vec<_>>();
            Value::materialize_all(&mut values);
            rows.push(TupleSlot::virtual_row(values));
        }
        state.left_rows = Some(rows);
    }

    loop {
        ctx.check_for_interrupts()?;
        if state.current_right.is_none() {
            match state.right.exec_proc_node(ctx)? {
                Some(slot) => {
                    let mut values = slot.values()?.iter().cloned().collect::<Vec<_>>();
                    Value::materialize_all(&mut values);
                    state.current_right = Some(TupleSlot::virtual_row(values));
                    state.left_index = 0;
                }
                None => return Ok(None),
            }
        }

        let left_rows = state.left_rows.as_ref().unwrap();
        while state.left_index < left_rows.len() {
            let li = state.left_index;
            state.left_index += 1;

            let left = &left_rows[li];
            let right = state.current_right.as_ref().unwrap();
            let mut combined_values: Vec<Value> = left.tts_values.clone();
            combined_values.extend(right.tts_values.iter().cloned());
            let nvalid = combined_values.len();
            state.slot.tts_values = combined_values;
            state.slot.tts_nvalid = nvalid;
            state.slot.kind = SlotKind::Virtual;
            state.slot.decode_offset = 0;

            match eval_expr(&state.on, &mut state.slot, ctx)? {
                Value::Bool(true) => return Ok(Some(&mut state.slot)),
                Value::Bool(false) | Value::Null => {}
                other => return Err(ExecError::NonBoolQual(other)),
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
        if self.rows.is_none() {
            let mut rows = Vec::new();
            while let Some(slot) = self.input.exec_proc_node(ctx)? {
                ctx.check_for_interrupts()?;
                let mut values = slot.values()?.iter().cloned().collect::<Vec<_>>();
                Value::materialize_all(&mut values);
                rows.push(TupleSlot::virtual_row(values));
            }

            let mut keyed_rows = Vec::with_capacity(rows.len());
            for mut row in rows {
                ctx.check_for_interrupts()?;
                let mut keys = Vec::with_capacity(self.items.len());
                for item in &self.items {
                    keys.push(eval_expr(&item.expr, &mut row, ctx)?);
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
            return Ok(None);
        }

        let idx = self.next_index;
        self.next_index += 1;
        Ok(Some(&mut rows[idx]))
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx)
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
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        format_explain_lines(&*self.input, indent, analyze, lines);
    }
}

impl PlanNode for LimitState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if let Some(limit) = self.limit {
            if self.returned >= limit {
                return Ok(None);
            }
        }

        while self.skipped < self.offset {
            ctx.check_for_interrupts()?;
            if self.input.exec_proc_node(ctx)?.is_none() {
                return Ok(None);
            }
            self.skipped += 1;
        }

        let slot = self.input.exec_proc_node(ctx)?;
        if slot.is_some() {
            self.returned += 1;
        }
        Ok(slot)
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        self.input.current_slot()
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
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        format_explain_lines(&*self.input, indent, analyze, lines);
    }
}

impl PlanNode for ProjectionState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let input_slot = match self.input.exec_proc_node(ctx)? {
            Some(s) => s,
            None => return Ok(None),
        };

        let mut values = Vec::with_capacity(self.targets.len());
        for target in &self.targets {
            values.push(eval_expr(&target.expr, input_slot, ctx)?.to_owned_value());
        }

        let nvalid = values.len();
        self.slot.tts_values = values;
        self.slot.tts_nvalid = nvalid;
        self.slot.kind = SlotKind::Virtual;
        self.slot.decode_offset = 0;
        Ok(Some(&mut self.slot))
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
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
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        format_explain_lines(&*self.input, indent, analyze, lines);
    }
}

impl PlanNode for AggregateState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.result_rows.is_none() {
            let mut groups: Vec<AggGroup> = Vec::new();

            while let Some(slot) = self.input.exec_proc_node(ctx)? {
                ctx.check_for_interrupts()?;
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
                        });
                        groups.len() - 1
                    });

                let group = &mut groups[group_idx];
                for (i, accum) in self.accumulators.iter().enumerate() {
                    let values = accum
                        .args
                        .iter()
                        .map(|arg| eval_expr(arg, slot, ctx))
                        .collect::<Result<Vec<_>, _>>()?;
                    (self.trans_fns[i])(&mut group.accum_states[i], &values);
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
                });
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
                    match eval_expr(having, &mut having_slot, ctx)? {
                        Value::Bool(true) => {}
                        Value::Bool(false) | Value::Null => continue,
                        other => return Err(ExecError::NonBoolQual(other)),
                    }
                }

                result_rows.push(TupleSlot::virtual_row(row_values));
            }

            self.result_rows = Some(result_rows);
        }

        let rows = self.result_rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            return Ok(None);
        }

        let idx = self.next_index;
        self.next_index += 1;
        Ok(Some(&mut rows[idx]))
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.result_rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx)
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
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        format_explain_lines(&*self.input, indent, analyze, lines);
    }
}

impl PlanNode for FunctionScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.rows.is_none() {
            let mut dummy = TupleSlot::empty(0);
            self.rows = Some(eval_set_returning_call(&self.call, &mut dummy, ctx)?);
        }

        let rows = self.rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            return Ok(None);
        }

        let idx = self.next_index;
        self.next_index += 1;
        Ok(Some(&mut rows[idx]))
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx)
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
    fn explain_children(&self, _indent: usize, _analyze: bool, _lines: &mut Vec<String>) {}
}

impl PlanNode for ValuesState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.result_rows.is_none() {
            let mut dummy = TupleSlot::empty(0);
            let rows = self
                .rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|expr| eval_expr(expr, &mut dummy, ctx))
                        .collect::<Result<Vec<_>, ExecError>>()
                        .map(TupleSlot::virtual_row)
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.result_rows = Some(rows);
        }

        let rows = self.result_rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            return Ok(None);
        }
        let idx = self.next_index;
        self.next_index += 1;
        Ok(Some(&mut rows[idx]))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.result_rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx)
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
    fn explain_children(&self, _indent: usize, _analyze: bool, _lines: &mut Vec<String>) {}
}

impl PlanNode for ProjectSetState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        loop {
            ctx.check_for_interrupts()?;
            if self.current_input.is_none() || self.next_index >= self.current_row_count {
                let Some(input_slot) = self.input.exec_proc_node(ctx)? else {
                    return Ok(None);
                };
                let mut values = input_slot.values()?.to_vec();
                Value::materialize_all(&mut values);
                let mut materialized = TupleSlot::virtual_row(values);
                let mut srf_rows = Vec::new();
                let mut max_rows = 0usize;
                for target in &self.targets {
                    if let crate::include::nodes::primnodes::ProjectSetTarget::Set {
                        call, ..
                    } = target
                    {
                        let rows = eval_scalar_set_returning_call(call, &mut materialized, ctx)?;
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

                self.current_input = Some(materialized);
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
                        values.push(eval_expr(&entry.expr, input_slot, ctx)?.to_owned_value());
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
            self.slot.tts_values = values;
            self.slot.tts_nvalid = self.slot.tts_values.len();
            self.slot.decode_offset = 0;

            if self.next_index >= self.current_row_count {
                self.current_input = None;
                self.current_srf_rows.clear();
                self.current_row_count = 0;
                self.next_index = 0;
            }

            return Ok(Some(&mut self.slot));
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
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
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        format_explain_lines(&*self.input, indent, analyze, lines);
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
