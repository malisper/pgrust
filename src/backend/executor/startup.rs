use super::agg::AccumState;
use super::{Plan, PlanState, TupleSlot, expr, tuple_decoder};
use crate::include::nodes::execnodes::{
    AggregateState, FilterState, FunctionScanState, IndexScanState, LimitState,
    NestedLoopJoinState, NodeExecStats, OrderByState, ProjectionState, ProjectSetState,
    ResultState, SeqScanState, ValuesState,
};

use std::rc::Rc;

pub fn executor_start(plan: Plan) -> PlanState {
    match plan {
        Plan::Result => Box::new(ResultState {
            emitted: false,
            slot: TupleSlot::empty(0),
            stats: NodeExecStats::default(),
        }),
        Plan::SeqScan { rel, desc, .. } => {
            let column_names: Vec<String> = desc.columns.iter().map(|c| c.name.clone()).collect();
            let attr_descs = desc.attribute_descs();
            let decoder = Rc::new(tuple_decoder::CompiledTupleDecoder::compile(
                &desc,
                &attr_descs,
            ));
            let ncols = desc.columns.len();
            let mut slot = TupleSlot::empty(ncols);
            slot.decoder = Some(decoder);
            Box::new(SeqScanState {
                rel,
                column_names,
                scan: None,
                slot,
                qual: None,
                stats: NodeExecStats::default(),
            })
        }
        Plan::IndexScan {
            rel,
            index_rel,
            am_oid,
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
                stats: NodeExecStats::default(),
            })
        }
        Plan::NestedLoopJoin { left, right, on } => {
            let combined_names: Vec<String> = left
                .column_names()
                .into_iter()
                .chain(right.column_names())
                .collect();
            let ncols = combined_names.len();
            Box::new(NestedLoopJoinState {
                left: executor_start(*left),
                right: executor_start(*right),
                on,
                combined_names,
                right_rows: None,
                current_left: None,
                right_index: 0,
                slot: TupleSlot::empty(ncols),
                stats: NodeExecStats::default(),
            })
        }
        Plan::Filter { input, predicate } if matches!(&*input, Plan::SeqScan { .. }) => {
            let Plan::SeqScan { rel, desc, .. } = *input else {
                unreachable!()
            };
            let column_names: Vec<String> = desc.columns.iter().map(|c| c.name.clone()).collect();
            let attr_descs = desc.attribute_descs();
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
                column_names,
                scan: None,
                slot,
                qual: Some(qual),
                stats: NodeExecStats::default(),
            })
        }
        Plan::Filter { input, predicate } => {
            let compiled_predicate = expr::compile_predicate(&predicate);
            Box::new(FilterState {
                input: executor_start(*input),
                predicate,
                compiled_predicate,
                stats: NodeExecStats::default(),
            })
        }
        Plan::OrderBy { input, items } => Box::new(OrderByState {
            input: executor_start(*input),
            items,
            rows: None,
            next_index: 0,
            stats: NodeExecStats::default(),
        }),
        Plan::Limit {
            input,
            limit,
            offset,
        } => Box::new(LimitState {
            input: executor_start(*input),
            limit,
            offset,
            skipped: 0,
            returned: 0,
            stats: NodeExecStats::default(),
        }),
        Plan::Projection { input, targets } => {
            let column_names: Vec<String> = targets.iter().map(|t| t.name.clone()).collect();
            let ncols = column_names.len();
            Box::new(ProjectionState {
                input: executor_start(*input),
                targets,
                column_names,
                slot: TupleSlot::empty(ncols),
                stats: NodeExecStats::default(),
            })
        }
        Plan::Aggregate {
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
                .map(|a| AccumState::transition_fn(a.func, a.args.len(), a.distinct))
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
                stats: NodeExecStats::default(),
            })
        }
        Plan::FunctionScan { call } => Box::new(FunctionScanState {
            output_columns: call.output_columns().iter().map(|c| c.name.clone()).collect(),
            call,
            rows: None,
            next_index: 0,
            stats: NodeExecStats::default(),
        }),
        Plan::Values {
            rows,
            output_columns,
        } => Box::new(ValuesState {
            rows,
            output_columns: output_columns.into_iter().map(|c| c.name).collect(),
            result_rows: None,
            next_index: 0,
            stats: NodeExecStats::default(),
        }),
        Plan::ProjectSet { input, targets } => {
            let column_names = targets
                .iter()
                .map(|target| match target {
                    crate::include::nodes::plannodes::ProjectSetTarget::Scalar(entry) => {
                        entry.name.clone()
                    }
                    crate::include::nodes::plannodes::ProjectSetTarget::Set { name, .. } => {
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
                stats: NodeExecStats::default(),
            })
        }
    }
}
