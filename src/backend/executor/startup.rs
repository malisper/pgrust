use super::agg::AccumState;
use super::{Plan, PlanState, TupleSlot, expr, tuple_decoder};
use crate::include::nodes::execnodes::{
    AggregateState, FilterState, GenerateSeriesState, JsonTableFunctionState, LimitState,
    NestedLoopJoinState, NodeExecStats, OrderByState, ProjectionState, ResultState, SeqScanState,
    UnnestState, ValuesState,
};

use std::rc::Rc;

pub fn executor_start(plan: Plan) -> PlanState {
    match plan {
        Plan::Result => Box::new(ResultState {
            emitted: false,
            slot: TupleSlot::empty(0),
            stats: NodeExecStats::default(),
        }),
        Plan::SeqScan { rel, desc } => {
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
            let Plan::SeqScan { rel, desc } = *input else {
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
        Plan::GenerateSeries {
            start,
            stop,
            step,
            output,
        } => Box::new(GenerateSeriesState {
            start,
            stop,
            step,
            output_type: output.sql_type,
            current: 0,
            end: 0,
            step_val: 0,
            num_current: None,
            num_end: None,
            num_step: None,
            initialized: false,
            slot: TupleSlot::empty(1),
            column_names: vec![output.name],
            stats: NodeExecStats::default(),
        }),
        Plan::Values { rows, output_columns } => Box::new(ValuesState {
            rows,
            output_columns: output_columns.into_iter().map(|c| c.name).collect(),
            result_rows: None,
            next_index: 0,
            stats: NodeExecStats::default(),
        }),
        Plan::Unnest {
            args,
            output_columns,
        } => {
            let column_names = output_columns.into_iter().map(|c| c.name).collect();
            Box::new(UnnestState {
                args,
                output_columns: column_names,
                rows: None,
                next_index: 0,
                stats: NodeExecStats::default(),
            })
        }
        Plan::JsonTableFunction {
            kind,
            arg,
            output_columns,
        } => {
            let column_names = output_columns.into_iter().map(|c| c.name).collect();
            Box::new(JsonTableFunctionState {
                kind,
                arg,
                output_columns: column_names,
                rows: None,
                next_index: 0,
                stats: NodeExecStats::default(),
            })
        }
    }
}
