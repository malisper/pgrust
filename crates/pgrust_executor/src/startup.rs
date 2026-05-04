use crate::srf::expr_uses_outer_columns;
use pgrust_nodes::primnodes::{
    AggAccum, OpExprKind, ProjectSetTarget, RowsFromSource, SetReturningCall, WindowFuncKind,
    expr_sql_type_hint, set_returning_call_exprs,
};
use pgrust_nodes::{Expr, Plan, SqlType, SqlTypeKind};
use std::collections::{HashMap, HashSet};

pub fn append_alias_prefix_from_relation_name(relation_name: &str) -> Option<String> {
    let alias = relation_name.split_whitespace().last()?;
    let (prefix, suffix) = alias.rsplit_once('_')?;
    (!prefix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_digit())).then(|| prefix.to_string())
}

pub fn append_sort_key_qualifier_from_plan(plan: &Plan) -> Option<String> {
    match plan {
        Plan::SeqScan { relation_name, .. }
        | Plan::TidScan { relation_name, .. }
        | Plan::IndexOnlyScan { relation_name, .. }
        | Plan::IndexScan { relation_name, .. }
        | Plan::BitmapHeapScan { relation_name, .. } => {
            append_alias_prefix_from_relation_name(relation_name)
        }
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. } => append_sort_key_qualifier_from_plan(input),
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::SetOp { children, .. } => children
            .iter()
            .find_map(append_sort_key_qualifier_from_plan),
        _ => None,
    }
}

pub fn plan_needs_network_strict_less_tiebreak(plan: &Plan) -> bool {
    match plan {
        Plan::Filter { predicate, .. } => expr_contains_network_strict_less(predicate),
        Plan::Projection { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. } => plan_needs_network_strict_less_tiebreak(input),
        _ => false,
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

pub fn recursive_union_distinct_hashable(sql_type: SqlType) -> bool {
    !matches!(
        sql_type.element_type().kind,
        SqlTypeKind::VarBit | SqlTypeKind::Json | SqlTypeKind::JsonPath
    )
}

pub fn plan_depends_on_worktable(plan: &Plan, worktable_id: usize) -> bool {
    match plan {
        Plan::WorkTableScan {
            worktable_id: scan_id,
            ..
        } => *scan_id == worktable_id,
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::SetOp { children, .. } => children
            .iter()
            .any(|child| plan_depends_on_worktable(child, worktable_id)),
        Plan::Unique { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. } => plan_depends_on_worktable(input, worktable_id),
        Plan::BitmapHeapScan { bitmapqual, .. } => {
            plan_depends_on_worktable(bitmapqual, worktable_id)
        }
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            plan_depends_on_worktable(left, worktable_id)
                || plan_depends_on_worktable(right, worktable_id)
        }
        Plan::CteScan { cte_plan, .. } => plan_depends_on_worktable(cte_plan, worktable_id),
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => {
            plan_depends_on_worktable(anchor, worktable_id)
                || plan_depends_on_worktable(recursive, worktable_id)
        }
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::TidScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::Values { .. } => false,
    }
}

fn plan_references_worktable(
    plan: &Plan,
    target_worktable_id: usize,
    cte_memo: &mut HashMap<usize, bool>,
) -> bool {
    match plan {
        Plan::WorkTableScan { worktable_id, .. } => *worktable_id == target_worktable_id,
        Plan::CteScan {
            cte_id, cte_plan, ..
        } => {
            if let Some(references_worktable) = cte_memo.get(cte_id) {
                return *references_worktable;
            }
            cte_memo.insert(*cte_id, false);
            let references_worktable =
                plan_references_worktable(cte_plan, target_worktable_id, cte_memo);
            cte_memo.insert(*cte_id, references_worktable);
            references_worktable
        }
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::SetOp { children, .. } => children
            .iter()
            .any(|child| plan_references_worktable(child, target_worktable_id, cte_memo)),
        Plan::BitmapHeapScan { bitmapqual, .. } => {
            plan_references_worktable(bitmapqual, target_worktable_id, cte_memo)
        }
        Plan::Unique { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. } => {
            plan_references_worktable(input, target_worktable_id, cte_memo)
        }
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            plan_references_worktable(left, target_worktable_id, cte_memo)
                || plan_references_worktable(right, target_worktable_id, cte_memo)
        }
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => {
            plan_references_worktable(anchor, target_worktable_id, cte_memo)
                || plan_references_worktable(recursive, target_worktable_id, cte_memo)
        }
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::TidScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::Values { .. } => false,
    }
}

fn collect_worktable_dependent_cte_ids(
    plan: &Plan,
    target_worktable_id: usize,
    ids: &mut HashSet<usize>,
    cte_memo: &mut HashMap<usize, bool>,
) {
    match plan {
        Plan::CteScan {
            cte_id, cte_plan, ..
        } => {
            if plan_references_worktable(cte_plan, target_worktable_id, cte_memo) {
                ids.insert(*cte_id);
            }
            collect_worktable_dependent_cte_ids(cte_plan, target_worktable_id, ids, cte_memo);
        }
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::SetOp { children, .. } => {
            for child in children {
                collect_worktable_dependent_cte_ids(child, target_worktable_id, ids, cte_memo);
            }
        }
        Plan::BitmapHeapScan { bitmapqual, .. } => {
            collect_worktable_dependent_cte_ids(bitmapqual, target_worktable_id, ids, cte_memo);
        }
        Plan::Unique { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. } => {
            collect_worktable_dependent_cte_ids(input, target_worktable_id, ids, cte_memo);
        }
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            collect_worktable_dependent_cte_ids(left, target_worktable_id, ids, cte_memo);
            collect_worktable_dependent_cte_ids(right, target_worktable_id, ids, cte_memo);
        }
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => {
            collect_worktable_dependent_cte_ids(anchor, target_worktable_id, ids, cte_memo);
            collect_worktable_dependent_cte_ids(recursive, target_worktable_id, ids, cte_memo);
        }
        Plan::Result { .. }
        | Plan::WorkTableScan { .. }
        | Plan::SeqScan { .. }
        | Plan::TidScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::Values { .. } => {}
    }
}

pub fn worktable_dependent_cte_ids(plan: &Plan, worktable_id: usize) -> Vec<usize> {
    let mut ids = HashSet::new();
    let mut cte_memo = HashMap::new();
    collect_worktable_dependent_cte_ids(plan, worktable_id, &mut ids, &mut cte_memo);
    let mut ids = ids.into_iter().collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

fn set_returning_call_uses_outer_columns(call: &SetReturningCall) -> bool {
    match call {
        SetReturningCall::RowsFrom { items, .. } => items.iter().any(|item| match &item.source {
            RowsFromSource::Function(call) => set_returning_call_uses_outer_columns(call),
            RowsFromSource::Project { output_exprs, .. } => {
                output_exprs.iter().any(expr_uses_outer_columns)
            }
        }),
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
        SetReturningCall::PgLockStatus { .. }
        | SetReturningCall::PgStatProgressCopy { .. }
        | SetReturningCall::PgSequences { .. }
        | SetReturningCall::InformationSchemaSequences { .. } => false,
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

fn agg_accum_uses_outer_columns(accum: &AggAccum) -> bool {
    accum.args.iter().any(expr_uses_outer_columns)
        || accum
            .order_by
            .iter()
            .any(|item| expr_uses_outer_columns(&item.expr))
        || accum.filter.as_ref().is_some_and(expr_uses_outer_columns)
}

fn project_set_target_uses_outer_columns(target: &ProjectSetTarget) -> bool {
    match target {
        ProjectSetTarget::Scalar(entry) => expr_uses_outer_columns(&entry.expr),
        ProjectSetTarget::Set { call, .. } => set_returning_call_uses_outer_columns(call),
    }
}

pub fn plan_uses_outer_columns(plan: &Plan) -> bool {
    match plan {
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::TidScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::BitmapOr { .. }
        | Plan::BitmapAnd { .. }
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
        Plan::Materialize { input, .. } => plan_uses_outer_columns(input),
        Plan::Memoize {
            input, cache_keys, ..
        } => plan_uses_outer_columns(input) || cache_keys.iter().any(expr_uses_outer_columns),
        Plan::Gather { input, .. } | Plan::GatherMerge { input, .. } => {
            plan_uses_outer_columns(input)
        }
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
            merge_key_descending: _,
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
                            WindowFuncKind::Aggregate(aggref) => {
                                aggref
                                    .aggfilter
                                    .as_ref()
                                    .is_some_and(expr_uses_outer_columns)
                                    || aggref
                                        .aggorder
                                        .iter()
                                        .any(|item| expr_uses_outer_columns(&item.expr))
                            }
                            WindowFuncKind::Builtin(_) => false,
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

fn expr_is_never_true(expr: &Expr) -> bool {
    match expr {
        Expr::Const(pgrust_nodes::Value::Bool(false)) | Expr::Const(pgrust_nodes::Value::Null) => {
            true
        }
        Expr::Bool(bool_expr) if bool_expr.boolop == pgrust_nodes::primnodes::BoolExprType::And => {
            bool_expr.args.iter().any(expr_is_never_true)
        }
        Expr::Bool(bool_expr) if bool_expr.boolop == pgrust_nodes::primnodes::BoolExprType::Or => {
            !bool_expr.args.is_empty() && bool_expr.args.iter().all(expr_is_never_true)
        }
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => expr_is_never_true(inner),
        _ => false,
    }
}

pub fn qual_list_is_never_true(quals: &[Expr]) -> bool {
    quals.iter().any(expr_is_never_true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_core::RelFileLocator;
    use pgrust_nodes::plannodes::PlanEstimate;
    use pgrust_nodes::primnodes::{BoolExpr, BoolExprType, OpExpr, Param, ParamKind, RelationDesc};

    #[test]
    fn append_alias_prefix_uses_numbered_suffix() {
        assert_eq!(
            append_alias_prefix_from_relation_name("public.t foo_12"),
            Some("foo".into())
        );
        assert_eq!(append_alias_prefix_from_relation_name("public.t foo"), None);
    }

    #[test]
    fn append_sort_key_qualifier_walks_through_filter() {
        let plan = Plan::Filter {
            plan_info: PlanEstimate::default(),
            predicate: Expr::Const(pgrust_nodes::Value::Bool(true)),
            input: Box::new(Plan::SeqScan {
                plan_info: PlanEstimate::default(),
                source_id: 1,
                rel: RelFileLocator {
                    spc_oid: 0,
                    db_oid: 0,
                    rel_number: 0,
                },
                relation_name: "public.t scan_3".into(),
                relation_oid: 1,
                relkind: 'r',
                relispopulated: true,
                toast: None,
                tablesample: None,
                desc: RelationDesc {
                    columns: Vec::new(),
                },
                disabled: false,
                parallel_aware: false,
            }),
        };

        assert_eq!(
            append_sort_key_qualifier_from_plan(&plan),
            Some("scan".into())
        );
    }

    #[test]
    fn recursive_hashability_excludes_json_and_varbit() {
        assert!(recursive_union_distinct_hashable(SqlType::new(
            SqlTypeKind::Int4
        )));
        assert!(!recursive_union_distinct_hashable(SqlType::new(
            SqlTypeKind::Json
        )));
        assert!(!recursive_union_distinct_hashable(SqlType::new(
            SqlTypeKind::VarBit
        )));
    }

    #[test]
    fn network_strict_less_tiebreak_detects_filter_lt() {
        let expr = Expr::Op(Box::new(OpExpr {
            op: OpExprKind::Lt,
            opno: 0,
            opfuncid: 0,
            args: vec![
                Expr::Cast(
                    Box::new(Expr::Const(pgrust_nodes::Value::Text("127.0.0.1".into()))),
                    SqlType::new(SqlTypeKind::Inet),
                ),
                Expr::Cast(
                    Box::new(Expr::Const(pgrust_nodes::Value::Text("127.0.0.2".into()))),
                    SqlType::new(SqlTypeKind::Inet),
                ),
            ],
            opresulttype: SqlType::new(SqlTypeKind::Bool),
            collation_oid: None,
        }));
        let plan = Plan::Filter {
            plan_info: PlanEstimate::default(),
            predicate: Expr::Bool(Box::new(BoolExpr {
                boolop: BoolExprType::And,
                args: vec![expr],
            })),
            input: Box::new(Plan::Result {
                plan_info: PlanEstimate::default(),
            }),
        };

        assert!(plan_needs_network_strict_less_tiebreak(&plan));
    }

    #[test]
    fn worktable_dependency_walks_cte_plans_and_sorts_ids() {
        let dependent_cte = Plan::CteScan {
            plan_info: PlanEstimate::default(),
            cte_id: 2,
            cte_name: "dependent".into(),
            cte_plan: Box::new(Plan::WorkTableScan {
                plan_info: PlanEstimate::default(),
                worktable_id: 7,
                output_columns: Vec::new(),
            }),
            output_columns: Vec::new(),
        };
        let independent_cte = Plan::CteScan {
            plan_info: PlanEstimate::default(),
            cte_id: 1,
            cte_name: "independent".into(),
            cte_plan: Box::new(Plan::Result {
                plan_info: PlanEstimate::default(),
            }),
            output_columns: Vec::new(),
        };
        let plan = Plan::Append {
            plan_info: PlanEstimate::default(),
            source_id: 0,
            desc: RelationDesc {
                columns: Vec::new(),
            },
            children: vec![dependent_cte, independent_cte],
            partition_prune: None,
            parallel_aware: false,
        };

        assert!(plan_depends_on_worktable(&plan, 7));
        assert_eq!(worktable_dependent_cte_ids(&plan, 7), vec![2]);
    }

    #[test]
    fn plan_outer_column_analysis_detects_params() {
        let plan = Plan::Filter {
            plan_info: PlanEstimate::default(),
            predicate: Expr::Param(Param {
                paramkind: ParamKind::External,
                paramid: 1,
                paramtype: SqlType::new(SqlTypeKind::Int4),
            }),
            input: Box::new(Plan::Result {
                plan_info: PlanEstimate::default(),
            }),
        };

        assert!(plan_uses_outer_columns(&plan));
    }

    #[test]
    fn qual_list_detects_never_true_boolean_shapes() {
        let qual = Expr::Bool(Box::new(BoolExpr {
            boolop: BoolExprType::Or,
            args: vec![Expr::Const(pgrust_nodes::Value::Bool(false))],
        }));

        assert!(qual_list_is_never_true(&[qual]));
    }
}
