use super::agg::AccumState;
use super::{Plan, PlanState, TupleSlot, expr, tuple_decoder};
use crate::backend::executor::hashjoin::HashJoinPhase;
use crate::include::catalog::bootstrap_catalog_kinds;
use crate::include::catalog::builtin_aggregate_function_for_proc_oid;
use crate::include::nodes::execnodes::{
    AggregateState, AppendState, FilterState, FunctionScanState, HashJoinState, HashState,
    IndexScanState, LimitState, NestedLoopJoinState, NodeExecStats, OrderByState, ProjectSetState,
    ProjectionState, ResultState, SeqScanState, ValuesState,
};
use crate::include::nodes::primnodes::{Expr, SetReturningCall};

use std::rc::Rc;

fn expr_uses_outer_columns(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varlevelsup > 0,
        Expr::OuterColumn { .. } => true,
        Expr::Aggref(aggref) => aggref.args.iter().any(expr_uses_outer_columns),
        Expr::Op(op) => op.args.iter().any(expr_uses_outer_columns),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_uses_outer_columns),
        Expr::Func(func) => func.args.iter().any(expr_uses_outer_columns),
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
        Expr::Cast(inner, _) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            expr_uses_outer_columns(inner)
        }
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
        Expr::Column(_)
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

fn set_returning_call_uses_outer_columns(call: &SetReturningCall) -> bool {
    match call {
        SetReturningCall::GenerateSeries {
            start, stop, step, ..
        } => {
            expr_uses_outer_columns(start)
                || expr_uses_outer_columns(stop)
                || expr_uses_outer_columns(step)
        }
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. }
        | SetReturningCall::UserDefined { args, .. } => args.iter().any(expr_uses_outer_columns),
    }
}

fn agg_accum_uses_outer_columns(accum: &crate::include::nodes::primnodes::AggAccum) -> bool {
    accum.args.iter().any(expr_uses_outer_columns)
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
        Plan::Result { .. } | Plan::SeqScan { .. } | Plan::IndexScan { .. } => false,
        Plan::Append { children, .. } => children.iter().any(plan_uses_outer_columns),
        Plan::Hash {
            input, hash_keys, ..
        } => plan_uses_outer_columns(input) || hash_keys.iter().any(expr_uses_outer_columns),
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
        Plan::Filter {
            input, predicate, ..
        } => plan_uses_outer_columns(input) || expr_uses_outer_columns(predicate),
        Plan::OrderBy { input, items, .. } => {
            plan_uses_outer_columns(input)
                || items.iter().any(|item| expr_uses_outer_columns(&item.expr))
        }
        Plan::Limit { input, .. } => plan_uses_outer_columns(input),
        Plan::Projection { input, targets, .. } => {
            plan_uses_outer_columns(input)
                || targets
                    .iter()
                    .any(|target| expr_uses_outer_columns(&target.expr))
        }
        Plan::Aggregate {
            input,
            group_by,
            accumulators,
            having,
            ..
        } => {
            plan_uses_outer_columns(input)
                || group_by.iter().any(expr_uses_outer_columns)
                || accumulators.iter().any(agg_accum_uses_outer_columns)
                || having.as_ref().is_some_and(expr_uses_outer_columns)
        }
        Plan::FunctionScan { call, .. } => set_returning_call_uses_outer_columns(call),
        Plan::Values { rows, .. } => rows.iter().flatten().any(expr_uses_outer_columns),
        Plan::ProjectSet { input, targets, .. } => {
            plan_uses_outer_columns(input)
                || targets.iter().any(project_set_target_uses_outer_columns)
        }
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
            desc,
            children,
        } => Box::new(AppendState {
            children: children.into_iter().map(executor_start).collect(),
            current_child: 0,
            column_names: desc.columns.iter().map(|c| c.name.clone()).collect(),
            slot: TupleSlot::empty(desc.columns.len()),
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::SeqScan {
            plan_info,
            rel,
            relation_oid,
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
                relation_name: explain_relation_name(relation_oid, rel.rel_number),
                toast_relation: toast,
                column_names,
                desc,
                attr_descs,
                scan: None,
                slot,
                qual: None,
                qual_expr: None,
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::IndexScan {
            plan_info,
            rel,
            index_rel,
            am_oid,
            toast,
            desc,
            index_meta,
            keys,
            direction,
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
            Box::new(IndexScanState {
                rel,
                toast_relation: toast,
                index_rel,
                am_oid,
                column_names,
                desc,
                attr_descs,
                index_meta,
                keys,
                direction,
                scan: None,
                slot,
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::Hash {
            plan_info,
            input,
            hash_keys,
        } => Box::new(build_hash_state(plan_info, *input, hash_keys)),
        Plan::NestedLoopJoin {
            plan_info,
            left,
            right,
            kind,
            join_qual,
            qual,
        } => {
            let right_plan = *right;
            let right_uses_outer = plan_uses_outer_columns(&right_plan);
            let cross_right_outer =
                matches!(kind, crate::include::nodes::primnodes::JoinType::Cross)
                    && !right_uses_outer
                    && !matches!(
                        &*left,
                        Plan::NestedLoopJoin {
                            kind: crate::include::nodes::primnodes::JoinType::Cross,
                            ..
                        }
                    );
            let left_width = left.column_names().len();
            let right_width = right_plan.column_names().len();
            let combined_names: Vec<String> = left
                .column_names()
                .into_iter()
                .chain(right_plan.column_names())
                .collect();
            let ncols = combined_names.len();
            Box::new(NestedLoopJoinState {
                left: executor_start(*left),
                right: executor_start(right_plan.clone()),
                right_plan: right_uses_outer.then_some(right_plan),
                kind,
                cross_right_outer,
                join_qual,
                qual,
                combined_names,
                left_rows: None,
                right_rows: None,
                right_matched: None,
                current_left: None,
                current_right: None,
                current_left_matched: false,
                left_index: 0,
                right_index: 0,
                left_width,
                right_width,
                unmatched_right_index: 0,
                slot: TupleSlot::empty(ncols),
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
                left_width,
                right_width,
                phase: HashJoinPhase::BuildHashTable,
                current_outer: None,
                current_bucket_entries: Vec::new(),
                current_bucket_index: 0,
                matched_outer: false,
                unmatched_inner_index: 0,
                slot: TupleSlot::empty(left_width + right_width),
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
                rel,
                relation_oid,
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
                relation_name: explain_relation_name(relation_oid, rel.rel_number),
                toast_relation: toast,
                column_names,
                desc,
                attr_descs,
                scan: None,
                slot,
                qual: Some(qual),
                qual_expr: Some(predicate),
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
        } => Box::new(OrderByState {
            input: executor_start(*input),
            items,
            rows: None,
            next_index: 0,
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
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::Aggregate {
            plan_info,
            input,
            group_by,
            accumulators,
            having,
            output_columns,
        } => {
            let output_column_names = output_columns.iter().map(|c| c.name.clone()).collect();
            let key_buffer = Vec::with_capacity(group_by.len());
            let trans_fns: Vec<fn(&mut AccumState, &[super::Value])> = accumulators
                .iter()
                .map(|a| {
                    let func =
                        builtin_aggregate_function_for_proc_oid(a.aggfnoid).unwrap_or_else(|| {
                            panic!(
                                "aggregate {:?} lacks builtin implementation mapping",
                                a.aggfnoid
                            )
                        });
                    AccumState::transition_fn(func, a.args.len(), a.distinct)
                })
                .collect();
            Box::new(AggregateState {
                input: executor_start(*input),
                group_by,
                accumulators,
                having,
                output_columns: output_column_names,
                result_rows: None,
                next_index: 0,
                key_buffer,
                trans_fns,
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::FunctionScan { plan_info, call } => Box::new(FunctionScanState {
            output_columns: call
                .output_columns()
                .iter()
                .map(|c| c.name.clone())
                .collect(),
            call,
            rows: None,
            next_index: 0,
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::Values {
            plan_info,
            rows,
            output_columns,
        } => Box::new(ValuesState {
            rows,
            output_columns: output_columns.into_iter().map(|c| c.name).collect(),
            result_rows: None,
            next_index: 0,
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

fn explain_relation_name(relation_oid: u32, relfilenode: u32) -> String {
    bootstrap_catalog_kinds()
        .into_iter()
        .find(|kind| kind.relation_oid() == relation_oid)
        .map(|kind| kind.relation_name().to_string())
        .unwrap_or_else(|| format!("rel {relfilenode}"))
}
