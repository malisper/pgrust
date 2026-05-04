use super::{Plan, PlanState, TupleSlot, expr, tuple_decoder};
use crate::backend::executor::hashjoin::HashJoinPhase;
use crate::include::nodes::execnodes::{
    AggregateState, AppendState, BitmapAndState, BitmapHeapScanState, BitmapIndexScanState,
    BitmapOrState, BitmapQualState, CteScanState, FilterState, FunctionScanState, GatherMergeState,
    GatherState, HashJoinState, HashState, IncrementalSortState, IndexOnlyScanState,
    IndexScanState, LimitState, LockRowsState, MaterializeState, MemoizeState, MergeAppendState,
    MergeJoinState, NestedLoopJoinState, NodeExecStats, OrderByState, ProjectSetState,
    ProjectionState, RecursiveUnionState, RecursiveWorkTable, ResultState, SeqScanState,
    SetOpState, SubqueryScanState, TableSampleState, TidScanState, UniqueState, ValuesState,
    WindowAggState, WorkTableScanState,
};
use crate::include::nodes::primnodes::Expr;
use pgrust_executor::{
    append_sort_key_qualifier_from_plan, plan_needs_network_strict_less_tiebreak,
    plan_uses_outer_columns, qual_list_is_never_true, recursive_union_distinct_hashable,
    worktable_dependent_cte_ids,
};

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

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
            parallel_aware,
            partition_prune,
            children,
        } => Box::new(AppendState {
            source_id,
            children: children.into_iter().map(executor_start).collect(),
            current_child: 0,
            parallel_aware,
            active_children: None,
            visible_children: None,
            subplans_removed: partition_prune
                .as_ref()
                .map(|info| info.subplans_removed)
                .unwrap_or_default(),
            partition_prune,
            column_names: desc.columns.iter().map(|c| c.name.clone()).collect(),
            slot: TupleSlot::empty(desc.columns.len()),
            current_bindings: Vec::new(),
            current_grouping_refs: Vec::new(),
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::MergeAppend {
            plan_info,
            source_id,
            desc,
            items,
            partition_prune,
            children,
        } => {
            let sort_key_qualifier = partition_prune.as_ref().and_then(|_| {
                children
                    .iter()
                    .find_map(append_sort_key_qualifier_from_plan)
            });
            Box::new(MergeAppendState {
                source_id,
                children: children.into_iter().map(executor_start).collect(),
                active_children: None,
                visible_children: None,
                subplans_removed: partition_prune
                    .as_ref()
                    .map(|info| info.subplans_removed)
                    .unwrap_or_default(),
                partition_prune,
                items,
                sort_key_qualifier,
                column_names: desc.columns.iter().map(|c| c.name.clone()).collect(),
                rows: None,
                next_index: 0,
                slot: TupleSlot::empty(desc.columns.len()),
                current_bindings: Vec::new(),
                current_grouping_refs: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
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
            current_grouping_refs: Vec::new(),
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::SeqScan {
            plan_info,
            source_id,
            parallel_scan_id,
            rel,
            relation_name,
            relation_oid,
            relkind,
            relispopulated,
            disabled,
            parallel_aware,
            toast,
            tablesample,
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
                parallel_aware,
                parallel_scan_id,
                toast_relation: toast,
                column_names,
                desc,
                attr_descs,
                scan: None,
                parallel_next_block: 0,
                tablesample: tablesample.map(TableSampleState::new),
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
        Plan::TidScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            relkind,
            relispopulated,
            toast,
            desc,
            tid_cond,
            filter,
        } => {
            let column_names: Vec<String> = desc.columns.iter().map(|c| c.name.clone()).collect();
            let desc = Rc::new(desc);
            let attr_descs: Rc<[_]> = desc.attribute_descs().into();
            let decoder = Rc::new(tuple_decoder::CompiledTupleDecoder::compile(
                &desc,
                &attr_descs,
            ));
            let qual = filter
                .as_ref()
                .map(|predicate| expr::compile_predicate_with_decoder(predicate, &decoder));
            let ncols = desc.columns.len();
            let mut slot = TupleSlot::empty(ncols);
            slot.decoder = Some(decoder);
            Box::new(TidScanState {
                rel,
                relation_name,
                relkind,
                relispopulated,
                toast_relation: toast,
                column_names,
                desc,
                attr_descs,
                tid_cond,
                candidates: Vec::new(),
                candidate_index: 0,
                candidates_initialized: false,
                slot,
                qual,
                qual_expr: filter,
                source_id,
                relation_oid,
                current_bindings: Vec::new(),
                current_page_pin: None,
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
            parallel_aware,
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
                parallel_aware,
                scan: None,
                pending_array_scan_keys: Vec::new(),
                array_scan_seen_tids: Default::default(),
                array_scan_keys_initialized: false,
                scan_exhausted: false,
                vm_buf: None,
                parallel_tuple_index: 0,
                slot,
                qual: None,
                qual_expr: None,
                source_id,
                relation_oid,
                current_bindings: Vec::new(),
                current_page_pin: None,
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
            parallel_aware,
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
                parallel_aware,
                scan: None,
                pending_array_scan_keys: Vec::new(),
                array_scan_seen_tids: Default::default(),
                array_scan_keys_initialized: false,
                scan_exhausted: false,
                parallel_tuple_index: 0,
                slot,
                qual: None,
                qual_expr: None,
                source_id,
                relation_oid,
                current_bindings: Vec::new(),
                current_page_pin: None,
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
        Plan::BitmapAnd {
            plan_info,
            children,
        } => Box::new(BitmapAndState {
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
            parallel_aware,
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
                parallel_aware,
                bitmap_pages: Vec::new(),
                current_page_index: 0,
                parallel_page_index: 0,
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
        Plan::Materialize { plan_info, input } => Box::new(MaterializeState {
            input: executor_start(*input),
            plan_info,
            stats: NodeExecStats::default(),
        }),
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
        Plan::Gather {
            plan_info,
            input,
            workers_planned,
            single_copy,
        } => Box::new(GatherState {
            slot: TupleSlot::empty(input.column_names().len()),
            input_plan: (*input).clone(),
            input: executor_start(*input),
            workers_planned,
            single_copy,
            initialized: false,
            receiver: None,
            worker_handles: Vec::new(),
            current_bindings: Vec::new(),
            current_grouping_refs: Vec::new(),
            leader_index: None,
            participant_count: 1,
            leader_done: false,
            parallel_runtime: None,
            workers_launched: 0,
            plan_info,
            stats: NodeExecStats::default(),
        }),
        Plan::GatherMerge {
            plan_info,
            input,
            workers_planned,
            items,
            display_items,
        } => Box::new(GatherMergeState {
            slot: TupleSlot::empty(input.column_names().len()),
            input_plan: (*input).clone(),
            input: executor_start(*input),
            workers_planned,
            items,
            display_items,
            initialized: false,
            rows: Vec::new(),
            next_row: 0,
            worker_handles: Vec::new(),
            current_bindings: Vec::new(),
            current_grouping_refs: Vec::new(),
            leader_index: None,
            participant_count: 1,
            leader_done: false,
            parallel_runtime: None,
            workers_launched: 0,
            plan_info,
            stats: NodeExecStats::default(),
        }),
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
                join_qual_never_matches: qual_list_is_never_true(&join_qual),
                join_qual,
                qual,
                combined_names,
                output_names,
                right_rows: None,
                right_matched: None,
                lateral_right_cache: std::collections::HashMap::new(),
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
            merge_key_descending,
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
                merge_key_descending,
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
                parallel_scan_id,
                rel,
                relation_name,
                relation_oid,
                relkind,
                relispopulated,
                disabled,
                parallel_aware,
                toast,
                tablesample,
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
                parallel_aware,
                parallel_scan_id,
                toast_relation: toast,
                column_names,
                desc,
                attr_descs,
                scan: None,
                parallel_next_block: 0,
                tablesample: tablesample.map(TableSampleState::new),
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
                parallel_aware,
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
                parallel_aware,
                scan: None,
                pending_array_scan_keys: Vec::new(),
                array_scan_seen_tids: Default::default(),
                array_scan_keys_initialized: false,
                scan_exhausted: false,
                vm_buf: None,
                parallel_tuple_index: 0,
                slot,
                qual: Some(qual),
                qual_expr: Some(predicate),
                source_id,
                relation_oid,
                current_bindings: Vec::new(),
                current_page_pin: None,
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
                parallel_aware,
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
                parallel_aware,
                scan: None,
                pending_array_scan_keys: Vec::new(),
                array_scan_seen_tids: Default::default(),
                array_scan_keys_initialized: false,
                scan_exhausted: false,
                parallel_tuple_index: 0,
                slot,
                qual: Some(qual),
                qual_expr: Some(predicate),
                source_id,
                relation_oid,
                current_bindings: Vec::new(),
                current_page_pin: None,
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
                current_grouping_refs: Vec::new(),
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
            limit_expr: limit,
            offset_expr: offset,
            limit: None,
            offset: 0,
            limits_ready: false,
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
                current_grouping_refs: Vec::new(),
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
            group_by_refs,
            grouping_sets,
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
                group_by_refs,
                grouping_sets,
                passthrough_exprs,
                accumulators,
                having,
                output_columns: output_column_names,
                result_rows: None,
                next_index: 0,
                key_buffer,
                runtimes: None,
                current_bindings: Vec::new(),
                current_grouping_refs: Vec::new(),
                plan_info,
                stats: NodeExecStats::default(),
            })
        }
        Plan::WindowAgg {
            plan_info,
            input,
            clause,
            run_condition,
            top_qual,
            output_columns,
        } => Box::new(WindowAggState {
            input: executor_start(*input),
            clause,
            run_condition,
            top_qual,
            output_columns: output_columns.into_iter().map(|c| c.name).collect(),
            result_rows: None,
            next_index: 0,
            current_bindings: Vec::new(),
            current_grouping_refs: Vec::new(),
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
            cte_name: _,
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
                current_grouping_refs: Vec::new(),
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
            let recursive_iteration_cte_ids =
                worktable_dependent_cte_ids(recursive.as_ref(), worktable_id);
            Box::new(RecursiveUnionState {
                worktable_id,
                distinct,
                distinct_hashable,
                recursive_references_worktable,
                recursive_iteration_cte_ids,
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
                current_grouping_refs: Vec::new(),
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
        instrumentation: Default::default(),
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
        Plan::BitmapAnd {
            plan_info,
            children,
        } => BitmapQualState::And(Box::new(BitmapAndState {
            children: children.into_iter().map(build_bitmap_qual_state).collect(),
            bitmap: crate::include::access::tidbitmap::TidBitmap::new(),
            executed: false,
            plan_info,
            stats: NodeExecStats::default(),
        })),
        other => panic!("bitmap heap scan requires bitmap index child, got {other:?}"),
    }
}
