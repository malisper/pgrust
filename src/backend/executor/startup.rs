use super::{Plan, PlanState, TupleSlot, expr, tuple_decoder};
use crate::backend::executor::hashjoin::HashJoinPhase;
use crate::backend::parser::SqlType;
use crate::include::nodes::execnodes::{
    AggregateState, AppendState, BitmapHeapScanState, BitmapIndexScanState, BitmapOrState,
    BitmapQualState, CteScanState, FilterState, FunctionScanState, HashJoinState, HashState,
    IncrementalSortState, IndexOnlyScanState, IndexScanState, LimitState, LockRowsState,
    MemoizeState, MergeAppendState, MergeJoinState, NestedLoopJoinState, NodeExecStats,
    OrderByState, ProjectSetState, ProjectionState, RecursiveUnionState, RecursiveWorkTable,
    ResultState, SeqScanState, SetOpState, SubqueryScanState, UniqueState, ValuesState,
    WindowAggState, WorkTableScanState,
};
use crate::include::nodes::parsenodes::SqlTypeKind;
use crate::include::nodes::primnodes::{
    Expr, OpExprKind, SetReturningCall, expr_sql_type_hint, set_returning_call_exprs,
};

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

fn expr_uses_outer_columns(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varlevelsup > 0,
        Expr::Param(_) => true,
        Expr::Aggref(aggref) => {
            aggref.args.iter().any(expr_uses_outer_columns)
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(expr_uses_outer_columns)
        }
        Expr::WindowFunc(window_func) => {
            window_func.args.iter().any(expr_uses_outer_columns)
                || match &window_func.kind {
                    crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) => aggref
                        .aggfilter
                        .as_ref()
                        .is_some_and(expr_uses_outer_columns),
                    crate::include::nodes::primnodes::WindowFuncKind::Builtin(_) => false,
                }
        }
        Expr::Op(op) => op.args.iter().any(expr_uses_outer_columns),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_uses_outer_columns),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_deref()
                .is_some_and(expr_uses_outer_columns)
                || case_expr.args.iter().any(|arm| {
                    expr_uses_outer_columns(&arm.expr) || expr_uses_outer_columns(&arm.result)
                })
                || expr_uses_outer_columns(&case_expr.defresult)
        }
        Expr::CaseTest(_) => false,
        Expr::Func(func) => func.args.iter().any(expr_uses_outer_columns),
        Expr::SqlJsonQueryFunction(func) => {
            func.child_exprs().into_iter().any(expr_uses_outer_columns)
        }
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .any(expr_uses_outer_columns),
        Expr::SubLink(sublink) => sublink
            .testexpr
            .as_deref()
            .is_some_and(expr_uses_outer_columns),
        Expr::SubPlan(subplan) => subplan
            .testexpr
            .as_deref()
            .is_some_and(expr_uses_outer_columns),
        Expr::ScalarArrayOp(saop) => {
            expr_uses_outer_columns(&saop.left) || expr_uses_outer_columns(&saop.right)
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => expr_uses_outer_columns(inner),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_uses_outer_columns(expr)
                || expr_uses_outer_columns(pattern)
                || escape.as_deref().is_some_and(expr_uses_outer_columns)
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_uses_outer_columns(left) || expr_uses_outer_columns(right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_uses_outer_columns),
        Expr::Row { fields, .. } => fields.iter().any(|(_, expr)| expr_uses_outer_columns(expr)),
        Expr::FieldSelect { expr, .. } => expr_uses_outer_columns(expr),
        Expr::ArraySubscript { array, subscripts } => {
            expr_uses_outer_columns(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(expr_uses_outer_columns)
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(expr_uses_outer_columns)
                })
        }
        Expr::Xml(xml) => xml.child_exprs().any(expr_uses_outer_columns),
        Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

fn expr_is_network_value(expr: &Expr) -> bool {
    expr_sql_type_hint(expr)
        .is_some_and(|sql_type| matches!(sql_type.kind, SqlTypeKind::Inet | SqlTypeKind::Cidr))
}

fn expr_contains_network_strict_less(expr: &Expr) -> bool {
    match expr {
        Expr::Op(op) if op.op == OpExprKind::Lt => op.args.iter().any(expr_is_network_value),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_contains_network_strict_less),
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            expr_contains_network_strict_less(inner)
        }
        _ => false,
    }
}

fn plan_needs_network_strict_less_tiebreak(plan: &Plan) -> bool {
    match plan {
        Plan::Filter { predicate, .. } => expr_contains_network_strict_less(predicate),
        Plan::Projection { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. } => plan_needs_network_strict_less_tiebreak(input),
        _ => false,
    }
}

fn recursive_union_distinct_hashable(sql_type: SqlType) -> bool {
    !matches!(
        sql_type.element_type().kind,
        SqlTypeKind::VarBit | SqlTypeKind::Json | SqlTypeKind::JsonPath | SqlTypeKind::Record
    )
}

fn set_returning_call_uses_outer_columns(call: &SetReturningCall) -> bool {
    match call {
        SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            timezone,
            ..
        } => {
            expr_uses_outer_columns(start)
                || expr_uses_outer_columns(stop)
                || expr_uses_outer_columns(step)
                || timezone.as_ref().is_some_and(expr_uses_outer_columns)
        }
        SetReturningCall::GenerateSubscripts {
            array,
            dimension,
            reverse,
            ..
        } => {
            expr_uses_outer_columns(array)
                || expr_uses_outer_columns(dimension)
                || reverse.as_ref().is_some_and(expr_uses_outer_columns)
        }
        SetReturningCall::PartitionTree { relid, .. }
        | SetReturningCall::PartitionAncestors { relid, .. } => expr_uses_outer_columns(relid),
        SetReturningCall::PgLockStatus { .. } => false,
        SetReturningCall::TxidSnapshotXip { arg, .. } => expr_uses_outer_columns(arg),
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::JsonRecordFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::StringTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. }
        | SetReturningCall::UserDefined { args, .. } => args.iter().any(expr_uses_outer_columns),
        SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_) => {
            set_returning_call_exprs(call)
                .iter()
                .any(|expr| expr_uses_outer_columns(expr))
        }
    }
}

fn agg_accum_uses_outer_columns(accum: &crate::include::nodes::primnodes::AggAccum) -> bool {
    accum.args.iter().any(expr_uses_outer_columns)
        || accum
            .order_by
            .iter()
            .any(|item| expr_uses_outer_columns(&item.expr))
        || accum.filter.as_ref().is_some_and(expr_uses_outer_columns)
}

fn project_set_target_uses_outer_columns(
    target: &crate::include::nodes::primnodes::ProjectSetTarget,
) -> bool {
    match target {
        crate::include::nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
            expr_uses_outer_columns(&entry.expr)
        }
        crate::include::nodes::primnodes::ProjectSetTarget::Set { call, .. } => {
            set_returning_call_uses_outer_columns(call)
        }
    }
}

fn plan_uses_outer_columns(plan: &Plan) -> bool {
    match plan {
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::BitmapOr { .. }
        | Plan::WorkTableScan { .. } => false,
        Plan::BitmapHeapScan {
            bitmapqual,
            recheck_qual,
            filter_qual,
            ..
        } => {
            plan_uses_outer_columns(bitmapqual)
                || recheck_qual.iter().any(expr_uses_outer_columns)
                || filter_qual.iter().any(expr_uses_outer_columns)
        }
        Plan::Append { children, .. } | Plan::SetOp { children, .. } => {
            children.iter().any(plan_uses_outer_columns)
        }
        Plan::MergeAppend {
            children, items, ..
        } => {
            children.iter().any(plan_uses_outer_columns)
                || items.iter().any(|item| expr_uses_outer_columns(&item.expr))
        }
        Plan::Unique { input, .. } => plan_uses_outer_columns(input),
        Plan::Hash {
            input, hash_keys, ..
        } => plan_uses_outer_columns(input) || hash_keys.iter().any(expr_uses_outer_columns),
        Plan::Memoize {
            input, cache_keys, ..
        } => plan_uses_outer_columns(input) || cache_keys.iter().any(expr_uses_outer_columns),
        Plan::NestedLoopJoin {
            left,
            right,
            join_qual,
            qual,
            ..
        } => {
            plan_uses_outer_columns(left)
                || plan_uses_outer_columns(right)
                || join_qual.iter().any(expr_uses_outer_columns)
                || qual.iter().any(expr_uses_outer_columns)
        }
        Plan::HashJoin {
            left,
            right,
            hash_clauses,
            hash_keys,
            join_qual,
            qual,
            ..
        } => {
            plan_uses_outer_columns(left)
                || plan_uses_outer_columns(right)
                || hash_clauses.iter().any(expr_uses_outer_columns)
                || hash_keys.iter().any(expr_uses_outer_columns)
                || join_qual.iter().any(expr_uses_outer_columns)
                || qual.iter().any(expr_uses_outer_columns)
        }
        Plan::MergeJoin {
            left,
            right,
            merge_clauses,
            outer_merge_keys,
            inner_merge_keys,
            join_qual,
            qual,
            ..
        } => {
            plan_uses_outer_columns(left)
                || plan_uses_outer_columns(right)
                || merge_clauses.iter().any(expr_uses_outer_columns)
                || outer_merge_keys.iter().any(expr_uses_outer_columns)
                || inner_merge_keys.iter().any(expr_uses_outer_columns)
                || join_qual.iter().any(expr_uses_outer_columns)
                || qual.iter().any(expr_uses_outer_columns)
        }
        Plan::Filter {
            input, predicate, ..
        } => plan_uses_outer_columns(input) || expr_uses_outer_columns(predicate),
        Plan::OrderBy { input, items, .. } => {
            plan_uses_outer_columns(input)
                || items.iter().any(|item| expr_uses_outer_columns(&item.expr))
        }
        Plan::IncrementalSort { input, items, .. } => {
            plan_uses_outer_columns(input)
                || items.iter().any(|item| expr_uses_outer_columns(&item.expr))
        }
        Plan::Limit { input, .. } | Plan::LockRows { input, .. } => plan_uses_outer_columns(input),
        Plan::Projection { input, targets, .. } => {
            plan_uses_outer_columns(input)
                || targets
                    .iter()
                    .any(|target| expr_uses_outer_columns(&target.expr))
        }
        Plan::Aggregate {
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            having,
            ..
        } => {
            plan_uses_outer_columns(input)
                || group_by.iter().any(expr_uses_outer_columns)
                || passthrough_exprs.iter().any(expr_uses_outer_columns)
                || accumulators.iter().any(agg_accum_uses_outer_columns)
                || having.as_ref().is_some_and(expr_uses_outer_columns)
        }
        Plan::WindowAgg { input, clause, .. } => {
            plan_uses_outer_columns(input)
                || clause.spec.partition_by.iter().any(expr_uses_outer_columns)
                || clause
                    .spec
                    .order_by
                    .iter()
                    .any(|item| expr_uses_outer_columns(&item.expr))
                || clause.functions.iter().any(|func| {
                    func.args.iter().any(expr_uses_outer_columns)
                        || match &func.kind {
                            crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) => {
                                aggref
                                    .aggfilter
                                    .as_ref()
                                    .is_some_and(expr_uses_outer_columns)
                                    || aggref
                                        .aggorder
                                        .iter()
                                        .any(|item| expr_uses_outer_columns(&item.expr))
                            }
                            crate::include::nodes::primnodes::WindowFuncKind::Builtin(_) => false,
                        }
                })
        }
        Plan::FunctionScan { call, .. } => set_returning_call_uses_outer_columns(call),
        Plan::SubqueryScan { input, .. } => plan_uses_outer_columns(input),
        Plan::Values { rows, .. } => rows.iter().flatten().any(expr_uses_outer_columns),
        Plan::ProjectSet { input, targets, .. } => {
            plan_uses_outer_columns(input)
                || targets.iter().any(project_set_target_uses_outer_columns)
        }
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => plan_uses_outer_columns(anchor) || plan_uses_outer_columns(recursive),
        Plan::CteScan { cte_plan, .. } => plan_uses_outer_columns(cte_plan),
    }
}

pub fn executor_start(plan: Plan) -> PlanState {
    match plan {
        Plan::Result { plan_info } => Box::new(ResultState {
            emitted: false,
            slot: TupleSlot::empty(0),
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::Append {
            plan_info,
            source_id,
            desc,
            children,
        } => Box::new(AppendState {
            source_id,
            children: children.into_iter().map(executor_start).collect(),
            current_child: 0,
            column_names: desc.columns.iter().map(|c| c.name.clone()).collect(),
            slot: TupleSlot::empty(desc.columns.len()),
            current_bindings: Vec::new(),
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::MergeAppend {
            plan_info,
            source_id,
            desc,
            items,
            children,
        } => Box::new(MergeAppendState {
            source_id,
            children: children.into_iter().map(executor_start).collect(),
            items,
            column_names: desc.columns.iter().map(|c| c.name.clone()).collect(),
            rows: None,
            next_index: 0,
            slot: TupleSlot::empty(desc.columns.len()),
            current_bindings: Vec::new(),
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::Unique {
            plan_info,
            key_indices,
            input,
        } => Box::new(UniqueState {
            input: executor_start(*input),
            key_indices,
            previous_values: None,
            slot: TupleSlot::empty(0),
            current_bindings: Vec::new(),
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::SeqScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            relkind,
            relispopulated,
            disabled,
            toast,
            desc,
        } => {
            let column_names: Vec<String> = desc.columns.iter().map(|c| c.name.clone()).collect();
            let desc = Rc::new(desc);
            let attr_descs: Rc<[_]> = desc.attribute_descs().into();
            let decoder = Rc::new(tuple_decoder::CompiledTupleDecoder::compile(
                &desc,
                &attr_descs,
            ));
            let ncols = desc.columns.len();
            let mut slot = TupleSlot::empty(ncols);
            slot.decoder = Some(decoder);
            Box::new(SeqScanState {
                rel,
                relation_name,
                relkind,
                relispopulated,
                disabled,
                toast_relation: toast,
                column_names,
                desc,
                attr_descs,
                scan: None,
                scan_rows: Vec::new(),
                scan_index: 0,
                sequence_emitted: false,
                slot,
                qual: None,
                qual_expr: None,
                source_id,
                relation_oid,
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::IndexOnlyScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            index_rel,
            index_name,
            am_oid,
            toast,
            desc,
            index_desc,
            index_meta,
            keys,
            order_by_keys,
            direction,
        } => {
            let column_names: Vec<String> = desc.columns.iter().map(|c| c.name.clone()).collect();
            let desc = Rc::new(desc);
            let index_desc = Rc::new(index_desc);
            let attr_descs: Rc<[_]> = desc.attribute_descs().into();
            let decoder = Rc::new(tuple_decoder::CompiledTupleDecoder::compile(
                &desc,
                &attr_descs,
            ));
            let ncols = desc.columns.len();
            let mut slot = TupleSlot::empty(ncols);
            slot.decoder = Some(decoder);
            Box::new(IndexOnlyScanState {
                rel,
                relation_name,
                toast_relation: toast,
                index_rel,
                index_name,
                am_oid,
                column_names,
                desc,
                index_desc,
                attr_descs,
                index_meta,
                keys,
                order_by_keys,
                direction,
                scan: None,
                scan_exhausted: false,
                vm_buf: None,
                slot,
                qual: None,
                qual_expr: None,
                source_id,
                relation_oid,
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::IndexScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            index_rel,
            index_name,
            am_oid,
            relation_oid,
            toast,
            desc,
            index_desc,
            index_meta,
            keys,
            order_by_keys,
            direction,
            index_only,
        } => {
            let column_names: Vec<String> = desc.columns.iter().map(|c| c.name.clone()).collect();
            let desc = Rc::new(desc);
            let index_desc = Rc::new(index_desc);
            let attr_descs: Rc<[_]> = desc.attribute_descs().into();
            let decoder = Rc::new(tuple_decoder::CompiledTupleDecoder::compile(
                &desc,
                &attr_descs,
            ));
            let ncols = desc.columns.len();
            let mut slot = TupleSlot::empty(ncols);
            slot.decoder = Some(decoder);
            Box::new(IndexScanState {
                rel,
                relation_name,
                toast_relation: toast,
                index_rel,
                index_name,
                am_oid,
                column_names,
                desc,
                index_desc,
                attr_descs,
                index_meta,
                keys,
                order_by_keys,
                direction,
                index_only,
                scan: None,
                scan_exhausted: false,
                slot,
                qual: None,
                qual_expr: None,
                source_id,
                relation_oid,
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::BitmapIndexScan {
            plan_info,
            source_id: _,
            rel,
            relation_oid: _,
            index_rel,
            index_name,
            am_oid,
            desc,
            index_desc,
            index_meta,
            keys,
            index_quals,
        } => Box::new(BitmapIndexScanState {
            rel,
            index_rel,
            index_name,
            am_oid,
            column_names: desc.columns.iter().map(|c| c.name.clone()).collect(),
            heap_desc: Rc::new(desc),
            index_desc: Rc::new(index_desc),
            index_meta,
            keys,
            index_quals,
            bitmap: crate::include::access::tidbitmap::TidBitmap::new(),
            executed: false,
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::BitmapOr {
            plan_info,
            children,
        } => Box::new(BitmapOrState {
            children: children.into_iter().map(build_bitmap_qual_state).collect(),
            bitmap: crate::include::access::tidbitmap::TidBitmap::new(),
            executed: false,
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::BitmapHeapScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            toast,
            desc,
            bitmapqual,
            recheck_qual,
            filter_qual,
        } => {
            let column_names: Vec<String> = desc.columns.iter().map(|c| c.name.clone()).collect();
            let desc = Rc::new(desc);
            let attr_descs: Rc<[_]> = desc.attribute_descs().into();
            let decoder = Rc::new(tuple_decoder::CompiledTupleDecoder::compile(
                &desc,
                &attr_descs,
            ));
            let ncols = desc.columns.len();
            let mut slot = TupleSlot::empty(ncols);
            slot.decoder = Some(decoder.clone());
            let recheck_qual = (!recheck_qual.is_empty()).then(|| {
                let mut quals = recheck_qual;
                let first = quals.remove(0);
                quals.into_iter().fold(first, Expr::and)
            });
            let compiled_recheck = recheck_qual
                .as_ref()
                .map(|qual| expr::compile_predicate_with_decoder(qual, &decoder));
            let filter_qual = (!filter_qual.is_empty()).then(|| {
                let mut quals = filter_qual;
                let first = quals.remove(0);
                quals.into_iter().fold(first, Expr::and)
            });
            let compiled_filter = filter_qual
                .as_ref()
                .map(|qual| expr::compile_predicate_with_decoder(qual, &decoder));
            Box::new(BitmapHeapScanState {
                rel,
                relation_name,
                toast_relation: toast,
                column_names,
                desc,
                attr_descs,
                bitmapqual: build_bitmap_qual_state(*bitmapqual),
                bitmap_pages: Vec::new(),
                current_page_index: 0,
                current_page_offsets: Vec::new(),
                current_offset_index: 0,
                current_page_pin: None,
                recheck_qual,
                compiled_recheck,
                filter_qual,
                compiled_filter,
                slot,
                source_id,
                relation_oid,
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::Hash {
            plan_info,
            input,
            hash_keys,
        } => Box::new(build_hash_state(plan_info, *input, hash_keys)),
        Plan::Memoize {
            plan_info,
            input,
            cache_keys,
            cache_key_labels,
            key_paramids,
            dependent_paramids,
            binary_mode,
            single_row,
            est_entries,
        } => {
            let input_plan = *input;
            let column_names = input_plan.column_names();
            let ncols = column_names.len();
            Box::new(MemoizeState {
                input: executor_start(input_plan.clone()),
                input_plan,
                cache_keys,
                cache_key_labels,
                key_paramids,
                dependent_paramids,
                binary_mode,
                single_row,
                est_entries,
                cache: HashMap::new(),
                lru: VecDeque::new(),
                active_rows: Vec::new(),
                active_index: 0,
                scan_prepared: false,
                last_nonkey_dependents: None,
                slot: TupleSlot::empty(ncols),
                current_bindings: Vec::new(),
                column_names,
                plan_info,
                stats: NodeExecStats::default(),
                memoize_stats: Default::default(),
            })
        }
        Plan::NestedLoopJoin {
            plan_info,
            left,
            right,
            kind,
            nest_params,
            join_qual,
            qual,
        } => {
            let right_plan = *right;
            let right_uses_outer = !nest_params.is_empty();
            let left_width = left.column_names().len();
            let right_width = right_plan.column_names().len();
            let combined_names: Vec<String> = left
                .column_names()
                .into_iter()
                .chain(right_plan.column_names())
                .collect();
            let output_names = if matches!(
                kind,
                crate::include::nodes::primnodes::JoinType::Semi
                    | crate::include::nodes::primnodes::JoinType::Anti
            ) {
                left.column_names()
            } else {
                combined_names.clone()
            };
            let ncols = output_names.len();
            Box::new(NestedLoopJoinState {
                left: executor_start(*left),
                right: executor_start(right_plan.clone()),
                right_plan: right_uses_outer.then_some(right_plan),
                kind,
                nest_params,
                join_qual,
                qual,
                combined_names,
                output_names,
                right_rows: None,
                right_matched: None,
                current_left: None,
                current_nest_param_saves: None,
                current_left_matched: false,
                right_index: 0,
                left_width,
                right_width,
                unmatched_right_index: 0,
                slot: TupleSlot::empty(ncols),
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::HashJoin {
            plan_info,
            left,
            right,
            kind,
            hash_clauses,
            hash_keys,
            join_qual,
            qual,
        } => {
            assert!(
                !matches!(kind, crate::include::nodes::primnodes::JoinType::Cross),
                "hash join does not support cross joins",
            );

            let Plan::Hash {
                plan_info: hash_plan_info,
                input: hash_input,
                hash_keys: inner_hash_keys,
            } = *right
            else {
                panic!("HashJoin right child must be Plan::Hash");
            };

            let left_width = left.column_names().len();
            let right_width = hash_input.column_names().len();
            let combined_names: Vec<String> = left
                .column_names()
                .into_iter()
                .chain(hash_input.column_names())
                .collect();
            let output_names = if matches!(
                kind,
                crate::include::nodes::primnodes::JoinType::Semi
                    | crate::include::nodes::primnodes::JoinType::Anti
            ) {
                left.column_names()
            } else {
                combined_names.clone()
            };

            Box::new(HashJoinState {
                left: executor_start(*left),
                right: Box::new(build_hash_state(
                    hash_plan_info,
                    *hash_input,
                    inner_hash_keys,
                )),
                kind,
                hash_clauses,
                hash_keys,
                join_qual,
                qual,
                combined_names,
                output_names,
                left_width,
                right_width,
                phase: HashJoinPhase::BuildHashTable,
                current_outer: None,
                current_bucket_entries: Vec::new(),
                current_bucket_index: 0,
                matched_outer: false,
                unmatched_inner_index: 0,
                slot: TupleSlot::empty(
                    if matches!(
                        kind,
                        crate::include::nodes::primnodes::JoinType::Semi
                            | crate::include::nodes::primnodes::JoinType::Anti
                    ) {
                        left_width
                    } else {
                        left_width + right_width
                    },
                ),
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::MergeJoin {
            plan_info,
            left,
            right,
            kind,
            merge_clauses,
            outer_merge_keys,
            inner_merge_keys,
            join_qual,
            qual,
        } => {
            assert!(
                !matches!(kind, crate::include::nodes::primnodes::JoinType::Cross),
                "merge join does not support cross joins",
            );
            let left_width = left.column_names().len();
            let right_width = right.column_names().len();
            let combined_names: Vec<String> = left
                .column_names()
                .into_iter()
                .chain(right.column_names())
                .collect();
            let output_names = if matches!(
                kind,
                crate::include::nodes::primnodes::JoinType::Semi
                    | crate::include::nodes::primnodes::JoinType::Anti
            ) {
                left.column_names()
            } else {
                combined_names.clone()
            };
            Box::new(MergeJoinState {
                left: executor_start(*left),
                right: executor_start(*right),
                kind,
                merge_clauses,
                outer_merge_keys,
                inner_merge_keys,
                join_qual,
                qual,
                combined_names,
                output_names,
                left_width,
                right_width,
                left_rows: None,
                right_rows: None,
                output_rows: None,
                next_output_index: 0,
                slot: TupleSlot::empty(
                    if matches!(
                        kind,
                        crate::include::nodes::primnodes::JoinType::Semi
                            | crate::include::nodes::primnodes::JoinType::Anti
                    ) {
                        left_width
                    } else {
                        left_width + right_width
                    },
                ),
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::Filter {
            plan_info,
            input,
            predicate,
        } if matches!(&*input, Plan::SeqScan { .. }) => {
            let Plan::SeqScan {
                plan_info: _,
                source_id,
                rel,
                relation_name,
                relation_oid,
                relkind,
                relispopulated,
                disabled,
                toast,
                desc,
            } = *input
            else {
                unreachable!()
            };
            let column_names: Vec<String> = desc.columns.iter().map(|c| c.name.clone()).collect();
            let desc = Rc::new(desc);
            let attr_descs: Rc<[_]> = desc.attribute_descs().into();
            let decoder = Rc::new(tuple_decoder::CompiledTupleDecoder::compile(
                &desc,
                &attr_descs,
            ));
            let qual = expr::compile_predicate_with_decoder(&predicate, &decoder);
            let ncols = desc.columns.len();
            let mut slot = TupleSlot::empty(ncols);
            slot.decoder = Some(decoder);
            Box::new(SeqScanState {
                rel,
                relation_name,
                relkind,
                relispopulated,
                disabled,
                toast_relation: toast,
                column_names,
                desc,
                attr_descs,
                scan: None,
                scan_rows: Vec::new(),
                scan_index: 0,
                sequence_emitted: false,
                slot,
                qual: Some(qual),
                qual_expr: Some(predicate),
                source_id,
                relation_oid,
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::Filter {
            plan_info,
            input,
            predicate,
        } if matches!(&*input, Plan::IndexOnlyScan { .. }) => {
            let Plan::IndexOnlyScan {
                plan_info: _,
                source_id,
                rel,
                relation_name,
                relation_oid,
                index_rel,
                index_name,
                am_oid,
                toast,
                desc,
                index_desc,
                index_meta,
                keys,
                order_by_keys,
                direction,
            } = *input
            else {
                unreachable!()
            };
            let column_names: Vec<String> = desc.columns.iter().map(|c| c.name.clone()).collect();
            let desc = Rc::new(desc);
            let index_desc = Rc::new(index_desc);
            let attr_descs: Rc<[_]> = desc.attribute_descs().into();
            let decoder = Rc::new(tuple_decoder::CompiledTupleDecoder::compile(
                &desc,
                &attr_descs,
            ));
            let qual = expr::compile_predicate_with_decoder(&predicate, &decoder);
            let ncols = desc.columns.len();
            let mut slot = TupleSlot::empty(ncols);
            slot.decoder = Some(decoder);
            Box::new(IndexOnlyScanState {
                rel,
                relation_name,
                toast_relation: toast,
                index_rel,
                index_name,
                am_oid,
                column_names,
                desc,
                index_desc,
                attr_descs,
                index_meta,
                keys,
                order_by_keys,
                direction,
                scan: None,
                scan_exhausted: false,
                vm_buf: None,
                slot,
                qual: Some(qual),
                qual_expr: Some(predicate),
                source_id,
                relation_oid,
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::Filter {
            plan_info,
            input,
            predicate,
        } if matches!(&*input, Plan::IndexScan { .. }) => {
            let Plan::IndexScan {
                plan_info: _,
                source_id,
                rel,
                relation_name,
                relation_oid,
                index_rel,
                index_name,
                am_oid,
                toast,
                desc,
                index_desc,
                index_meta,
                keys,
                order_by_keys,
                direction,
                index_only,
            } = *input
            else {
                unreachable!()
            };
            let column_names: Vec<String> = desc.columns.iter().map(|c| c.name.clone()).collect();
            let desc = Rc::new(desc);
            let index_desc = Rc::new(index_desc);
            let attr_descs: Rc<[_]> = desc.attribute_descs().into();
            let decoder = Rc::new(tuple_decoder::CompiledTupleDecoder::compile(
                &desc,
                &attr_descs,
            ));
            let qual = expr::compile_predicate_with_decoder(&predicate, &decoder);
            let ncols = desc.columns.len();
            let mut slot = TupleSlot::empty(ncols);
            slot.decoder = Some(decoder);
            Box::new(IndexScanState {
                rel,
                relation_name,
                toast_relation: toast,
                index_rel,
                index_name,
                am_oid,
                column_names,
                desc,
                index_desc,
                attr_descs,
                index_meta,
                keys,
                order_by_keys,
                direction,
                index_only,
                scan: None,
                scan_exhausted: false,
                slot,
                qual: Some(qual),
                qual_expr: Some(predicate),
                source_id,
                relation_oid,
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::Filter {
            plan_info,
            input,
            predicate,
        } => {
            let compiled_predicate = expr::compile_predicate(&predicate);
            Box::new(FilterState {
                input: executor_start(*input),
                predicate,
                compiled_predicate,
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::OrderBy {
            plan_info,
            input,
            items,
            display_items,
        } => {
            let network_strict_less_tiebreak = plan_needs_network_strict_less_tiebreak(&input);
            Box::new(OrderByState {
                input: executor_start(*input),
                items,
                display_items,
                network_strict_less_tiebreak,
                rows: None,
                next_index: 0,
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::IncrementalSort {
            plan_info,
            input,
            items,
            presorted_count,
            display_items,
            presorted_display_items,
        } => Box::new(IncrementalSortState {
            input: executor_start(*input),
            items,
            presorted_count,
            display_items,
            presorted_display_items,
            rows: Vec::new(),
            next_index: 0,
            lookahead: None,
            current_bindings: Vec::new(),
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::Limit {
            plan_info,
            input,
            limit,
            offset,
        } => Box::new(LimitState {
            input: executor_start(*input),
            limit,
            offset,
            skipped: 0,
            returned: 0,
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::LockRows {
            plan_info,
            input,
            row_marks,
        } => Box::new(LockRowsState {
            input: executor_start(*input),
            row_marks,
            current_bindings: Vec::new(),
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::Projection {
            plan_info,
            input,
            targets,
        } => {
            let column_names: Vec<String> = targets.iter().map(|t| t.name.clone()).collect();
            let ncols = column_names.len();
            Box::new(ProjectionState {
                input: executor_start(*input),
                targets,
                column_names,
                slot: TupleSlot::empty(ncols),
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::Aggregate {
            plan_info,
            strategy,
            phase,
            disabled,
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            semantic_accumulators: _,
            semantic_output_names: _,
            having,
            output_columns,
        } => {
            let output_column_names = output_columns.iter().map(|c| c.name.clone()).collect();
            let key_buffer = Vec::with_capacity(group_by.len());
            Box::new(AggregateState {
                input: executor_start(*input),
                strategy,
                phase,
                disabled,
                group_by,
                passthrough_exprs,
                accumulators,
                having,
                output_columns: output_column_names,
                result_rows: None,
                next_index: 0,
                key_buffer,
                runtimes: None,
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::WindowAgg {
            plan_info,
            input,
            clause,
            output_columns,
        } => Box::new(WindowAggState {
            input: executor_start(*input),
            clause,
            output_columns: output_columns.into_iter().map(|c| c.name).collect(),
            result_rows: None,
            next_index: 0,
            current_bindings: Vec::new(),
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::FunctionScan {
            plan_info,
            call,
            table_alias,
        } => {
            let output_columns = call
                .output_columns()
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>();
            Box::new(FunctionScanState {
                slot: TupleSlot::empty(output_columns.len()),
                output_columns,
                call,
                table_alias,
                rows: None,
                next_index: 0,
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::SubqueryScan {
            plan_info,
            input,
            scan_name: _,
            filter,
            output_columns,
        } => Box::new(SubqueryScanState {
            input: executor_start(*input),
            compiled_filter: filter.as_ref().map(expr::compile_predicate),
            filter,
            output_columns: output_columns.into_iter().map(|c| c.name).collect(),
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::CteScan {
            plan_info,
            cte_id,
            cte_plan,
            output_columns,
        } => {
            let width = output_columns.len();
            Box::new(CteScanState {
                cte_id,
                cte_plan: *cte_plan,
                output_columns: output_columns.into_iter().map(|c| c.name).collect(),
                next_index: 0,
                slot: TupleSlot::empty(width),
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::WorkTableScan {
            plan_info,
            worktable_id,
            output_columns,
        } => {
            let width = output_columns.len();
            Box::new(WorkTableScanState {
                worktable_id,
                output_columns: output_columns.into_iter().map(|c| c.name).collect(),
                next_index: 0,
                slot: TupleSlot::empty(width),
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::RecursiveUnion {
            plan_info,
            worktable_id,
            distinct,
            recursive_references_worktable,
            output_columns,
            anchor,
            recursive,
        } => {
            let width = output_columns.len();
            let distinct_hashable = output_columns
                .iter()
                .all(|column| recursive_union_distinct_hashable(column.sql_type));
            Box::new(RecursiveUnionState {
                worktable_id,
                distinct,
                distinct_hashable,
                recursive_references_worktable,
                anchor: executor_start(*anchor),
                recursive_plan: *recursive,
                recursive_state: None,
                output_columns: output_columns.into_iter().map(|c| c.name).collect(),
                worktable: Rc::new(std::cell::RefCell::new(RecursiveWorkTable::default())),
                intermediate_rows: Vec::new(),
                seen_rows: std::collections::HashSet::new(),
                anchor_done: false,
                slot: TupleSlot::empty(width),
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::SetOp {
            plan_info,
            op,
            strategy,
            output_columns,
            children,
        } => {
            let width = output_columns.len();
            Box::new(SetOpState {
                op,
                strategy,
                children: children.into_iter().map(executor_start).collect(),
                output_columns: output_columns.into_iter().map(|c| c.name).collect(),
                result_rows: None,
                next_index: 0,
                slot: TupleSlot::empty(width),
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::Values {
            plan_info,
            rows,
            output_columns,
        } => Box::new(ValuesState {
            rows,
            output_columns: output_columns.into_iter().map(|c| c.name).collect(),
            result_rows: None,
            next_index: 0,
            current_bindings: Vec::new(),
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::ProjectSet {
            plan_info,
            input,
            targets,
        } => {
            let column_names = targets
                .iter()
                .map(|target| match target {
                    crate::include::nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
                        entry.name.clone()
                    }
                    crate::include::nodes::primnodes::ProjectSetTarget::Set { name, .. } => {
                        name.clone()
                    }
                })
                .collect::<Vec<_>>();
            Box::new(ProjectSetState {
                input: executor_start(*input),
                targets,
                output_columns: column_names.clone(),
                current_input: None,
                current_srf_rows: Vec::new(),
                current_row_count: 0,
                next_index: 0,
                slot: TupleSlot::empty(column_names.len()),
                current_bindings: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
    }
}

fn build_hash_state(
    plan_info: crate::include::nodes::plannodes::PlanEstimate,
    input: Plan,
    hash_keys: Vec<crate::include::nodes::primnodes::Expr>,
) -> HashState {
    let column_names = input.column_names();
    HashState {
        input: executor_start(input),
        hash_keys,
        column_names,
        table: None,
        built: false,
        plan_info,
        stats: NodeExecStats::default(),
    }
}

fn build_bitmap_qual_state(plan: Plan) -> BitmapQualState {
    match plan {
        Plan::BitmapIndexScan {
            plan_info,
            source_id: _,
            rel,
            relation_oid: _,
            index_rel,
            index_name,
            am_oid,
            desc,
            index_desc,
            index_meta,
            keys,
            index_quals,
        } => BitmapQualState::Index(Box::new(BitmapIndexScanState {
            rel,
            index_rel,
            index_name,
            am_oid,
            column_names: desc.columns.iter().map(|c| c.name.clone()).collect(),
            heap_desc: Rc::new(desc),
            index_desc: Rc::new(index_desc),
            index_meta,
            keys,
            index_quals,
            bitmap: crate::include::access::tidbitmap::TidBitmap::new(),
            executed: false,
            plan_info,
            stats: NodeExecStats::default(),
        })),
        Plan::BitmapOr {
            plan_info,
            children,
        } => BitmapQualState::Or(Box::new(BitmapOrState {
            children: children.into_iter().map(build_bitmap_qual_state).collect(),
            bitmap: crate::include::access::tidbitmap::TidBitmap::new(),
            executed: false,
            plan_info,
            stats: NodeExecStats::default(),
        })),
        other => panic!("bitmap heap scan requires bitmap index child, got {other:?}"),
    }
}
