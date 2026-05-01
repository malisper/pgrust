use std::collections::BTreeSet;

use crate::backend::parser::CatalogLookup;
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{JoinTreeNode, Query, RangeTblEntryKind};
use crate::include::nodes::pathnodes::PlannerInfo;
use crate::include::nodes::primnodes::{
    Expr, ScalarFunctionImpl, WindowFuncKind, attrno_index, expr_contains_set_returning,
    set_returning_call_exprs,
};

use super::super::joininfo;

pub(super) fn prune_unused_subquery_outputs(
    mut query: Query,
    used_attrs: &BTreeSet<usize>,
    catalog: &dyn CatalogLookup,
) -> Query {
    if query.set_operation.is_some() || query.distinct || query.has_target_srfs {
        return query;
    }
    let internally_used_refs = query
        .sort_clause
        .iter()
        .chain(query.distinct_on.iter())
        .map(|clause| clause.tle_sort_group_ref)
        .filter(|sortgroupref| *sortgroupref != 0)
        .collect::<BTreeSet<_>>();

    for (index, target) in query.target_list.iter_mut().enumerate() {
        if used_attrs.contains(&index)
            || internally_used_refs.contains(&target.ressortgroupref)
            || expr_contains_set_returning(&target.expr)
            || expr_contains_prune_volatile(&target.expr, catalog)
        {
            continue;
        }
        target.expr = Expr::Const(Value::Null);
    }
    query
}

pub(super) fn used_parent_attrs_for_rte(
    root: &PlannerInfo,
    rtindex: usize,
    width: usize,
) -> BTreeSet<usize> {
    let mut used = BTreeSet::new();
    for target in &root.parse.target_list {
        collect_parent_expr_used_attrs(root, rtindex, width, &target.expr, &mut used);
    }
    if let Some(where_qual) = root.parse.where_qual.as_ref() {
        collect_parent_expr_used_attrs(root, rtindex, width, where_qual, &mut used);
    }
    for expr in &root.parse.group_by {
        collect_parent_expr_used_attrs(root, rtindex, width, expr, &mut used);
    }
    for grouping_set in &root.parse.grouping_sets {
        for ref_id in grouping_set {
            if let Some(index) = root
                .parse
                .group_by_refs
                .iter()
                .position(|candidate| candidate == ref_id)
            {
                collect_parent_expr_used_attrs(
                    root,
                    rtindex,
                    width,
                    &root.parse.group_by[index],
                    &mut used,
                );
            }
        }
    }
    for accum in &root.parse.accumulators {
        for expr in accum.direct_args.iter().chain(accum.args.iter()) {
            collect_parent_expr_used_attrs(root, rtindex, width, expr, &mut used);
        }
        for item in &accum.order_by {
            collect_parent_expr_used_attrs(root, rtindex, width, &item.expr, &mut used);
        }
        if let Some(filter) = accum.filter.as_ref() {
            collect_parent_expr_used_attrs(root, rtindex, width, filter, &mut used);
        }
    }
    if let Some(having) = root.parse.having_qual.as_ref() {
        collect_parent_expr_used_attrs(root, rtindex, width, having, &mut used);
    }
    for item in &root.parse.sort_clause {
        collect_parent_expr_used_attrs(root, rtindex, width, &item.expr, &mut used);
    }
    for item in &root.parse.distinct_on {
        collect_parent_expr_used_attrs(root, rtindex, width, &item.expr, &mut used);
    }
    if let Some(jointree) = root.parse.jointree.as_ref() {
        collect_parent_jointree_used_attrs(root, rtindex, width, jointree, &mut used);
    }
    for rte in &root.parse.rtable {
        match &rte.kind {
            RangeTblEntryKind::Values { rows, .. } => {
                for expr in rows.iter().flatten() {
                    collect_parent_expr_used_attrs(root, rtindex, width, expr, &mut used);
                }
            }
            RangeTblEntryKind::Function { call } => {
                for expr in set_returning_call_exprs(call) {
                    collect_parent_expr_used_attrs(root, rtindex, width, expr, &mut used);
                }
            }
            RangeTblEntryKind::Subquery { query }
                if query_contains_outer_ref_to_rte(query, rtindex, 1)
                    || query_contains_outer_join_alias_ref_to_rte(&root.parse, query, rtindex) =>
            {
                // :HACK: PostgreSQL tracks these with PlaceHolderVars and
                // nullable-rel metadata. Until pgrust has that full machinery,
                // keep all outputs from an RTE that is referenced by a sibling
                // LATERAL subquery so pruning cannot replace needed projection
                // expressions with NULL.
                used.extend(0..width);
            }
            RangeTblEntryKind::Result
            | RangeTblEntryKind::Relation { .. }
            | RangeTblEntryKind::Join { .. }
            | RangeTblEntryKind::WorkTable { .. }
            | RangeTblEntryKind::Cte { .. }
            | RangeTblEntryKind::Subquery { .. } => {}
        }
    }
    used
}

fn query_contains_outer_join_alias_ref_to_rte(
    parent_query: &Query,
    query: &Query,
    rtindex: usize,
) -> bool {
    parent_query.rtable.iter().enumerate().any(|(index, rte)| {
        let RangeTblEntryKind::Join { joinaliasvars, .. } = &rte.kind else {
            return false;
        };
        joinaliasvars
            .iter()
            .any(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, 0))
            && query_contains_outer_ref_to_rte(query, index + 1, 1)
    })
}

fn query_contains_outer_ref_to_rte(query: &Query, rtindex: usize, target_level: usize) -> bool {
    query
        .target_list
        .iter()
        .any(|target| expr_contains_outer_ref_to_rte(&target.expr, rtindex, target_level))
        || query
            .where_qual
            .as_ref()
            .is_some_and(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level))
        || query
            .group_by
            .iter()
            .any(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level))
        || query.accumulators.iter().any(|accum| {
            accum
                .direct_args
                .iter()
                .chain(accum.args.iter())
                .any(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level))
                || accum
                    .order_by
                    .iter()
                    .any(|item| expr_contains_outer_ref_to_rte(&item.expr, rtindex, target_level))
                || accum
                    .filter
                    .as_ref()
                    .is_some_and(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level))
        })
        || query
            .having_qual
            .as_ref()
            .is_some_and(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level))
        || query
            .sort_clause
            .iter()
            .any(|item| expr_contains_outer_ref_to_rte(&item.expr, rtindex, target_level))
        || query
            .distinct_on
            .iter()
            .any(|item| expr_contains_outer_ref_to_rte(&item.expr, rtindex, target_level))
        || query.jointree.as_ref().is_some_and(|jointree| {
            jointree_contains_outer_ref_to_rte(jointree, rtindex, target_level)
        })
        || query.rtable.iter().any(|rte| match &rte.kind {
            RangeTblEntryKind::Values { rows, .. } => rows
                .iter()
                .flatten()
                .any(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level)),
            RangeTblEntryKind::Function { call } => set_returning_call_exprs(call)
                .into_iter()
                .any(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level)),
            RangeTblEntryKind::Subquery { query } | RangeTblEntryKind::Cte { query, .. } => {
                query_contains_outer_ref_to_rte(query, rtindex, target_level + 1)
            }
            RangeTblEntryKind::Result
            | RangeTblEntryKind::Relation { .. }
            | RangeTblEntryKind::Join { .. }
            | RangeTblEntryKind::WorkTable { .. } => false,
        })
}

fn jointree_contains_outer_ref_to_rte(
    node: &JoinTreeNode,
    rtindex: usize,
    target_level: usize,
) -> bool {
    match node {
        JoinTreeNode::RangeTblRef(_) => false,
        JoinTreeNode::JoinExpr {
            left, right, quals, ..
        } => {
            jointree_contains_outer_ref_to_rte(left, rtindex, target_level)
                || jointree_contains_outer_ref_to_rte(right, rtindex, target_level)
                || expr_contains_outer_ref_to_rte(quals, rtindex, target_level)
        }
    }
}

fn expr_contains_outer_ref_to_rte(expr: &Expr, rtindex: usize, target_level: usize) -> bool {
    match expr {
        Expr::Var(var) => var.varlevelsup == target_level && var.varno == rtindex,
        Expr::Aggref(aggref) => {
            aggref
                .direct_args
                .iter()
                .chain(aggref.args.iter())
                .any(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level))
                || aggref
                    .aggorder
                    .iter()
                    .any(|item| expr_contains_outer_ref_to_rte(&item.expr, rtindex, target_level))
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level))
        }
        Expr::GroupingKey(grouping_key) => {
            expr_contains_outer_ref_to_rte(&grouping_key.expr, rtindex, target_level)
        }
        Expr::GroupingFunc(grouping_func) => grouping_func
            .args
            .iter()
            .any(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level)),
        Expr::WindowFunc(func) => {
            func.args
                .iter()
                .any(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level))
                || match &func.kind {
                    WindowFuncKind::Aggregate(aggref) => expr_contains_outer_ref_to_rte(
                        &Expr::Aggref(Box::new(aggref.clone())),
                        rtindex,
                        target_level,
                    ),
                    WindowFuncKind::Builtin(_) => false,
                }
        }
        Expr::Op(op) => op
            .args
            .iter()
            .any(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level)),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_some_and(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level))
                || case_expr.args.iter().any(|arm| {
                    expr_contains_outer_ref_to_rte(&arm.expr, rtindex, target_level)
                        || expr_contains_outer_ref_to_rte(&arm.result, rtindex, target_level)
                })
                || expr_contains_outer_ref_to_rte(&case_expr.defresult, rtindex, target_level)
        }
        Expr::Func(func) => func
            .args
            .iter()
            .any(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level)),
        Expr::SqlJsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
            .any(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level)),
        Expr::SetReturning(set_returning) => set_returning_call_exprs(&set_returning.call)
            .into_iter()
            .any(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level)),
        Expr::SubLink(sublink) => {
            sublink
                .testexpr
                .as_ref()
                .is_some_and(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level))
                || query_contains_outer_ref_to_rte(&sublink.subselect, rtindex, target_level + 1)
        }
        Expr::SubPlan(subplan) => {
            subplan
                .testexpr
                .as_ref()
                .is_some_and(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level))
                || subplan
                    .args
                    .iter()
                    .any(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level))
        }
        Expr::ScalarArrayOp(saop) => {
            expr_contains_outer_ref_to_rte(&saop.left, rtindex, target_level)
                || expr_contains_outer_ref_to_rte(&saop.right, rtindex, target_level)
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .any(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level)),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => {
            expr_contains_outer_ref_to_rte(inner, rtindex, target_level)
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
            expr_contains_outer_ref_to_rte(expr, rtindex, target_level)
                || expr_contains_outer_ref_to_rte(pattern, rtindex, target_level)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level))
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_contains_outer_ref_to_rte(left, rtindex, target_level)
                || expr_contains_outer_ref_to_rte(right, rtindex, target_level)
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .any(|expr| expr_contains_outer_ref_to_rte(expr, rtindex, target_level)),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_contains_outer_ref_to_rte(expr, rtindex, target_level)),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_outer_ref_to_rte(array, rtindex, target_level)
                || subscripts.iter().any(|subscript| {
                    subscript.lower.as_ref().is_some_and(|expr| {
                        expr_contains_outer_ref_to_rte(expr, rtindex, target_level)
                    }) || subscript.upper.as_ref().is_some_and(|expr| {
                        expr_contains_outer_ref_to_rte(expr, rtindex, target_level)
                    })
                })
        }
        Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::User
        | Expr::SystemUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

fn collect_parent_jointree_used_attrs(
    root: &PlannerInfo,
    rtindex: usize,
    width: usize,
    node: &JoinTreeNode,
    used: &mut BTreeSet<usize>,
) {
    match node {
        JoinTreeNode::RangeTblRef(_) => {}
        JoinTreeNode::JoinExpr {
            left, right, quals, ..
        } => {
            collect_parent_jointree_used_attrs(root, rtindex, width, left, used);
            collect_parent_jointree_used_attrs(root, rtindex, width, right, used);
            collect_parent_expr_used_attrs(root, rtindex, width, quals, used);
        }
    }
}

fn collect_parent_expr_used_attrs(
    root: &PlannerInfo,
    rtindex: usize,
    width: usize,
    expr: &Expr,
    used: &mut BTreeSet<usize>,
) {
    let expr = joininfo::flatten_join_alias_vars_query(&root.parse, expr.clone());
    collect_expr_used_attrs(&expr, rtindex, width, used);
}

fn collect_expr_used_attrs(expr: &Expr, rtindex: usize, width: usize, used: &mut BTreeSet<usize>) {
    match expr {
        // LATERAL function arguments are bound against previous FROM items as
        // one-level outer Vars, but they still consume attributes from this
        // parent query's RTE for subquery-output pruning.
        Expr::Var(var) if var.varlevelsup <= 1 && var.varno == rtindex => {
            if let Some(index) = attrno_index(var.varattno) {
                used.insert(index);
            } else {
                used.extend(0..width);
            }
        }
        Expr::Aggref(aggref) => {
            for expr in aggref.direct_args.iter().chain(aggref.args.iter()) {
                collect_expr_used_attrs(expr, rtindex, width, used);
            }
            for item in &aggref.aggorder {
                collect_expr_used_attrs(&item.expr, rtindex, width, used);
            }
            if let Some(filter) = aggref.aggfilter.as_ref() {
                collect_expr_used_attrs(filter, rtindex, width, used);
            }
        }
        Expr::GroupingKey(grouping_key) => {
            collect_expr_used_attrs(&grouping_key.expr, rtindex, width, used);
        }
        Expr::GroupingFunc(grouping_func) => {
            for expr in &grouping_func.args {
                collect_expr_used_attrs(expr, rtindex, width, used);
            }
        }
        Expr::WindowFunc(func) => {
            for expr in &func.args {
                collect_expr_used_attrs(expr, rtindex, width, used);
            }
            if let WindowFuncKind::Aggregate(aggref) = &func.kind {
                for expr in aggref.direct_args.iter().chain(aggref.args.iter()) {
                    collect_expr_used_attrs(expr, rtindex, width, used);
                }
                for item in &aggref.aggorder {
                    collect_expr_used_attrs(&item.expr, rtindex, width, used);
                }
                if let Some(filter) = aggref.aggfilter.as_ref() {
                    collect_expr_used_attrs(filter, rtindex, width, used);
                }
            }
        }
        Expr::Op(op) => {
            for arg in &op.args {
                collect_expr_used_attrs(arg, rtindex, width, used);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &bool_expr.args {
                collect_expr_used_attrs(arg, rtindex, width, used);
            }
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = case_expr.arg.as_ref() {
                collect_expr_used_attrs(arg, rtindex, width, used);
            }
            for arm in &case_expr.args {
                collect_expr_used_attrs(&arm.expr, rtindex, width, used);
                collect_expr_used_attrs(&arm.result, rtindex, width, used);
            }
            collect_expr_used_attrs(&case_expr.defresult, rtindex, width, used);
        }
        Expr::Func(func) => {
            for arg in &func.args {
                collect_expr_used_attrs(arg, rtindex, width, used);
            }
        }
        Expr::SqlJsonQueryFunction(func) => {
            for child in func.child_exprs() {
                collect_expr_used_attrs(child, rtindex, width, used);
            }
        }
        Expr::SetReturning(set_returning) => {
            for expr in set_returning_call_exprs(&set_returning.call) {
                collect_expr_used_attrs(expr, rtindex, width, used);
            }
        }
        Expr::SubLink(_) | Expr::SubPlan(_) => {
            used.extend(0..width);
        }
        Expr::ScalarArrayOp(saop) => {
            collect_expr_used_attrs(&saop.left, rtindex, width, used);
            collect_expr_used_attrs(&saop.right, rtindex, width, used);
        }
        Expr::Xml(xml) => {
            for child in xml.child_exprs() {
                collect_expr_used_attrs(child, rtindex, width, used);
            }
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => {
            collect_expr_used_attrs(inner, rtindex, width, used);
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
            collect_expr_used_attrs(expr, rtindex, width, used);
            collect_expr_used_attrs(pattern, rtindex, width, used);
            if let Some(escape) = escape.as_ref() {
                collect_expr_used_attrs(escape, rtindex, width, used);
            }
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_expr_used_attrs(left, rtindex, width, used);
            collect_expr_used_attrs(right, rtindex, width, used);
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_expr_used_attrs(element, rtindex, width, used);
            }
        }
        Expr::Row { fields, .. } => {
            for (_, expr) in fields {
                collect_expr_used_attrs(expr, rtindex, width, used);
            }
        }
        Expr::ArraySubscript { array, subscripts } => {
            collect_expr_used_attrs(array, rtindex, width, used);
            for subscript in subscripts {
                if let Some(lower) = subscript.lower.as_ref() {
                    collect_expr_used_attrs(lower, rtindex, width, used);
                }
                if let Some(upper) = subscript.upper.as_ref() {
                    collect_expr_used_attrs(upper, rtindex, width, used);
                }
            }
        }
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::User
        | Expr::SystemUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => {}
    }
}

fn expr_contains_prune_volatile(expr: &Expr, catalog: &dyn CatalogLookup) -> bool {
    match expr {
        Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => true,
        Expr::Op(op) => {
            let proc_oid = op
                .opfuncid
                .ne(&0)
                .then_some(op.opfuncid)
                .or_else(|| catalog.operator_by_oid(op.opno).map(|row| row.oprcode));
            let op_is_volatile = proc_oid
                .map(|oid| proc_is_volatile(oid, catalog))
                .unwrap_or(op.opno != 0);
            op_is_volatile
                || op
                    .args
                    .iter()
                    .any(|arg| expr_contains_prune_volatile(arg, catalog))
        }
        Expr::Func(func) => {
            matches!(
                func.implementation,
                ScalarFunctionImpl::UserDefined { proc_oid } if proc_is_volatile(proc_oid, catalog)
            ) || func
                .args
                .iter()
                .any(|arg| expr_contains_prune_volatile(arg, catalog))
        }
        Expr::Aggref(aggref) => {
            aggref
                .direct_args
                .iter()
                .chain(aggref.args.iter())
                .any(|arg| expr_contains_prune_volatile(arg, catalog))
                || aggref
                    .aggorder
                    .iter()
                    .any(|item| expr_contains_prune_volatile(&item.expr, catalog))
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(|filter| expr_contains_prune_volatile(filter, catalog))
        }
        Expr::GroupingKey(grouping_key) => {
            expr_contains_prune_volatile(&grouping_key.expr, catalog)
        }
        Expr::GroupingFunc(grouping_func) => grouping_func
            .args
            .iter()
            .any(|arg| expr_contains_prune_volatile(arg, catalog)),
        Expr::WindowFunc(func) => {
            func.args
                .iter()
                .any(|arg| expr_contains_prune_volatile(arg, catalog))
                || match &func.kind {
                    WindowFuncKind::Aggregate(aggref) => {
                        aggref
                            .direct_args
                            .iter()
                            .chain(aggref.args.iter())
                            .any(|arg| expr_contains_prune_volatile(arg, catalog))
                            || aggref
                                .aggorder
                                .iter()
                                .any(|item| expr_contains_prune_volatile(&item.expr, catalog))
                            || aggref
                                .aggfilter
                                .as_ref()
                                .is_some_and(|filter| expr_contains_prune_volatile(filter, catalog))
                    }
                    WindowFuncKind::Builtin(_) => false,
                }
        }
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(|arg| expr_contains_prune_volatile(arg, catalog)),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_some_and(|expr| expr_contains_prune_volatile(expr, catalog))
                || case_expr.args.iter().any(|arm| {
                    expr_contains_prune_volatile(&arm.expr, catalog)
                        || expr_contains_prune_volatile(&arm.result, catalog)
                })
                || expr_contains_prune_volatile(&case_expr.defresult, catalog)
        }
        Expr::SqlJsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
            .any(|expr| expr_contains_prune_volatile(expr, catalog)),
        Expr::SetReturning(_) => true,
        Expr::SubLink(sublink) => {
            sublink
                .testexpr
                .as_ref()
                .is_some_and(|expr| expr_contains_prune_volatile(expr, catalog))
                || query_contains_prune_volatile(&sublink.subselect, catalog)
        }
        Expr::SubPlan(subplan) => {
            subplan
                .testexpr
                .as_ref()
                .is_some_and(|expr| expr_contains_prune_volatile(expr, catalog))
                || subplan
                    .args
                    .iter()
                    .any(|arg| expr_contains_prune_volatile(arg, catalog))
        }
        Expr::ScalarArrayOp(saop) => {
            expr_contains_prune_volatile(&saop.left, catalog)
                || expr_contains_prune_volatile(&saop.right, catalog)
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .any(|expr| expr_contains_prune_volatile(expr, catalog)),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => expr_contains_prune_volatile(inner, catalog),
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
            expr_contains_prune_volatile(expr, catalog)
                || expr_contains_prune_volatile(pattern, catalog)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_contains_prune_volatile(expr, catalog))
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_contains_prune_volatile(left, catalog)
                || expr_contains_prune_volatile(right, catalog)
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .any(|expr| expr_contains_prune_volatile(expr, catalog)),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_contains_prune_volatile(expr, catalog)),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_prune_volatile(array, catalog)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(|expr| expr_contains_prune_volatile(expr, catalog))
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(|expr| expr_contains_prune_volatile(expr, catalog))
                })
        }
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::User
        | Expr::SystemUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema => false,
    }
}

fn query_contains_prune_volatile(query: &Query, catalog: &dyn CatalogLookup) -> bool {
    query
        .target_list
        .iter()
        .any(|target| expr_contains_prune_volatile(&target.expr, catalog))
        || query
            .where_qual
            .as_ref()
            .is_some_and(|expr| expr_contains_prune_volatile(expr, catalog))
        || query
            .group_by
            .iter()
            .any(|expr| expr_contains_prune_volatile(expr, catalog))
        || query
            .accumulators
            .iter()
            .any(|accum| agg_accum_contains_prune_volatile(accum, catalog))
        || query
            .having_qual
            .as_ref()
            .is_some_and(|expr| expr_contains_prune_volatile(expr, catalog))
        || query
            .sort_clause
            .iter()
            .chain(query.distinct_on.iter())
            .any(|item| expr_contains_prune_volatile(&item.expr, catalog))
        || query.rtable.iter().any(|rte| match &rte.kind {
            RangeTblEntryKind::Values { rows, .. } => rows
                .iter()
                .flatten()
                .any(|expr| expr_contains_prune_volatile(expr, catalog)),
            RangeTblEntryKind::Function { call } => set_returning_call_exprs(call)
                .into_iter()
                .any(|expr| expr_contains_prune_volatile(&expr, catalog)),
            RangeTblEntryKind::Cte { query, .. } | RangeTblEntryKind::Subquery { query } => {
                query_contains_prune_volatile(query, catalog)
            }
            RangeTblEntryKind::Result
            | RangeTblEntryKind::Relation { .. }
            | RangeTblEntryKind::Join { .. }
            | RangeTblEntryKind::WorkTable { .. } => false,
        })
        || query.recursive_union.as_ref().is_some_and(|recursive| {
            query_contains_prune_volatile(&recursive.anchor, catalog)
                || query_contains_prune_volatile(&recursive.recursive, catalog)
        })
        || query.set_operation.as_ref().is_some_and(|set_operation| {
            set_operation
                .inputs
                .iter()
                .any(|input| query_contains_prune_volatile(input, catalog))
        })
}

fn agg_accum_contains_prune_volatile(
    accum: &crate::include::nodes::primnodes::AggAccum,
    catalog: &dyn CatalogLookup,
) -> bool {
    accum
        .direct_args
        .iter()
        .chain(accum.args.iter())
        .any(|arg| expr_contains_prune_volatile(arg, catalog))
        || accum
            .order_by
            .iter()
            .any(|item| expr_contains_prune_volatile(&item.expr, catalog))
        || accum
            .filter
            .as_ref()
            .is_some_and(|filter| expr_contains_prune_volatile(filter, catalog))
}

fn proc_is_volatile(proc_oid: u32, catalog: &dyn CatalogLookup) -> bool {
    catalog
        .proc_row_by_oid(proc_oid)
        .is_none_or(|row| row.provolatile == 'v')
}
