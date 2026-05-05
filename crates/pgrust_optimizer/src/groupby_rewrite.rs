use std::collections::{BTreeMap, BTreeSet};

use pgrust_analyze::CatalogLookup;
use pgrust_nodes::parsenodes::{JoinTreeNode, Query, RangeTblEntryKind};
use pgrust_nodes::pathnodes::AggregateLayout;
use pgrust_nodes::primnodes::{
    AttrNumber, Expr, JoinType, OpExprKind, Var, WindowClause, WindowFrameBound, WindowFuncExpr,
    WindowFuncKind, set_returning_call_exprs,
};

use super::joininfo::flatten_join_alias_vars_query;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct VarKey {
    varno: usize,
    varattno: AttrNumber,
    varlevelsup: usize,
}

#[derive(Debug, Clone, Copy)]
struct GroupVarInfo {
    relid: usize,
    attno: i16,
}

pub fn build_aggregate_layout(query: &Query, catalog: &dyn CatalogLookup) -> AggregateLayout {
    let original_group_by = query.group_by.clone();
    let original_group_by_refs = if query.group_by_refs.len() == original_group_by.len() {
        query.group_by_refs.clone()
    } else {
        (1..=original_group_by.len()).collect()
    };
    let reduced_group_by = if original_group_by.len() < 2
        || !query.grouping_sets.is_empty()
        || query_has_outer_joins(query)
    {
        original_group_by.clone()
    } else {
        let per_relation_reduced = remove_redundant_relation_group_keys(query, catalog);
        collapse_duplicate_group_keys(query, per_relation_reduced)
    };

    let mut passthrough_exprs = collect_passthrough_exprs(query, &reduced_group_by);
    collect_aggregate_passthrough_exprs(query, &reduced_group_by, &mut passthrough_exprs);
    let group_by_refs = refs_for_reduced_group_by(
        &original_group_by,
        &original_group_by_refs,
        &reduced_group_by,
    );

    AggregateLayout {
        group_by: reduced_group_by,
        group_by_refs,
        passthrough_exprs,
    }
}

fn refs_for_reduced_group_by(
    original_group_by: &[Expr],
    original_group_by_refs: &[usize],
    reduced_group_by: &[Expr],
) -> Vec<usize> {
    let mut used = Vec::new();
    reduced_group_by
        .iter()
        .enumerate()
        .map(|(reduced_index, expr)| {
            let original_index = original_group_by
                .iter()
                .enumerate()
                .find(|(index, original)| *original == expr && !used.contains(index))
                .map(|(index, _)| index)
                .unwrap_or(reduced_index);
            used.push(original_index);
            original_group_by_refs
                .get(original_index)
                .copied()
                .unwrap_or(reduced_index + 1)
        })
        .collect()
}

fn collect_passthrough_exprs(query: &Query, reduced_group_by: &[Expr]) -> Vec<Expr> {
    query
        .group_by
        .iter()
        .filter(|expr| {
            !reduced_group_by.contains(expr) && query_references_group_output_expr(query, expr)
        })
        .cloned()
        .collect()
}

fn collect_aggregate_passthrough_exprs(
    query: &Query,
    group_by: &[Expr],
    passthrough_exprs: &mut Vec<Expr>,
) {
    for target in &query.target_list {
        collect_passthrough_expr(&target.expr, group_by, passthrough_exprs);
    }
    if let Some(having) = query.having_qual.as_ref() {
        collect_passthrough_expr(having, group_by, passthrough_exprs);
    }
    for item in &query.sort_clause {
        collect_passthrough_expr(&item.expr, group_by, passthrough_exprs);
    }
    for clause in &query.window_clauses {
        for expr in &clause.spec.partition_by {
            collect_passthrough_expr(expr, group_by, passthrough_exprs);
        }
        for item in &clause.spec.order_by {
            collect_passthrough_expr(&item.expr, group_by, passthrough_exprs);
        }
        for func in &clause.functions {
            collect_window_func_passthrough_exprs(func, group_by, passthrough_exprs);
        }
    }
}

fn push_passthrough_expr(exprs: &mut Vec<Expr>, expr: Expr) {
    if !exprs.contains(&expr) {
        exprs.push(expr);
    }
}

fn collect_window_func_passthrough_exprs(
    func: &WindowFuncExpr,
    group_by: &[Expr],
    passthrough_exprs: &mut Vec<Expr>,
) {
    for arg in &func.args {
        collect_passthrough_expr(arg, group_by, passthrough_exprs);
    }
    if let WindowFuncKind::Aggregate(aggref) = &func.kind {
        for item in &aggref.aggorder {
            collect_passthrough_expr(&item.expr, group_by, passthrough_exprs);
        }
        if let Some(filter) = aggref.aggfilter.as_ref() {
            collect_passthrough_expr(filter, group_by, passthrough_exprs);
        }
    }
}

fn collect_passthrough_expr(expr: &Expr, group_by: &[Expr], passthrough_exprs: &mut Vec<Expr>) {
    if group_by.contains(expr) {
        return;
    }
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 => {
            push_passthrough_expr(passthrough_exprs, expr.clone());
        }
        Expr::Aggref(_) => {}
        Expr::GroupingKey(grouping_key) => {
            collect_passthrough_expr(&grouping_key.expr, group_by, passthrough_exprs);
        }
        Expr::GroupingFunc(grouping_func) => {
            for arg in &grouping_func.args {
                collect_passthrough_expr(arg, group_by, passthrough_exprs);
            }
        }
        Expr::WindowFunc(window_func) => {
            collect_window_func_passthrough_exprs(window_func, group_by, passthrough_exprs);
        }
        Expr::Op(op) => {
            for arg in &op.args {
                collect_passthrough_expr(arg, group_by, passthrough_exprs);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &bool_expr.args {
                collect_passthrough_expr(arg, group_by, passthrough_exprs);
            }
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_passthrough_expr(arg, group_by, passthrough_exprs);
            }
            for arm in &case_expr.args {
                collect_passthrough_expr(&arm.expr, group_by, passthrough_exprs);
                collect_passthrough_expr(&arm.result, group_by, passthrough_exprs);
            }
            collect_passthrough_expr(&case_expr.defresult, group_by, passthrough_exprs);
        }
        Expr::Func(func) => {
            for arg in &func.args {
                collect_passthrough_expr(arg, group_by, passthrough_exprs);
            }
        }
        Expr::SqlJsonQueryFunction(func) => {
            for child in func.child_exprs() {
                collect_passthrough_expr(child, group_by, passthrough_exprs);
            }
        }
        Expr::SetReturning(srf) => {
            for arg in set_returning_call_exprs(&srf.call) {
                collect_passthrough_expr(arg, group_by, passthrough_exprs);
            }
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_passthrough_expr(testexpr, group_by, passthrough_exprs);
            }
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                collect_passthrough_expr(testexpr, group_by, passthrough_exprs);
            }
            for arg in &subplan.args {
                collect_passthrough_expr(arg, group_by, passthrough_exprs);
            }
        }
        Expr::ScalarArrayOp(op) => {
            collect_passthrough_expr(&op.left, group_by, passthrough_exprs);
            collect_passthrough_expr(&op.right, group_by, passthrough_exprs);
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => collect_passthrough_expr(inner, group_by, passthrough_exprs),
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
            collect_passthrough_expr(expr, group_by, passthrough_exprs);
            collect_passthrough_expr(pattern, group_by, passthrough_exprs);
            if let Some(escape) = escape.as_deref() {
                collect_passthrough_expr(escape, group_by, passthrough_exprs);
            }
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_passthrough_expr(left, group_by, passthrough_exprs);
            collect_passthrough_expr(right, group_by, passthrough_exprs);
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_passthrough_expr(element, group_by, passthrough_exprs);
            }
        }
        Expr::Row { fields, .. } => {
            for (_, expr) in fields {
                collect_passthrough_expr(expr, group_by, passthrough_exprs);
            }
        }
        Expr::FieldSelect { expr, .. } => {
            collect_passthrough_expr(expr, group_by, passthrough_exprs);
        }
        Expr::ArraySubscript { array, subscripts } => {
            collect_passthrough_expr(array, group_by, passthrough_exprs);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_passthrough_expr(lower, group_by, passthrough_exprs);
                }
                if let Some(upper) = &subscript.upper {
                    collect_passthrough_expr(upper, group_by, passthrough_exprs);
                }
            }
        }
        Expr::Xml(xml) => {
            for child in xml.child_exprs() {
                collect_passthrough_expr(child, group_by, passthrough_exprs);
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

fn query_references_group_output_expr(query: &Query, target: &Expr) -> bool {
    query
        .target_list
        .iter()
        .any(|entry| expr_references_group_output(&entry.expr, target))
        || query
            .having_qual
            .as_ref()
            .is_some_and(|expr| expr_references_group_output(expr, target))
        || query
            .sort_clause
            .iter()
            .any(|item| expr_references_group_output(&item.expr, target))
        || query
            .window_clauses
            .iter()
            .any(|clause| window_clause_references_group_output(clause, target))
}

fn window_clause_references_group_output(clause: &WindowClause, target: &Expr) -> bool {
    clause
        .functions
        .iter()
        .any(|func| window_func_references_group_output(func, target))
        || clause
            .spec
            .partition_by
            .iter()
            .any(|expr| expr_references_group_output(expr, target))
        || clause
            .spec
            .order_by
            .iter()
            .any(|item| expr_references_group_output(&item.expr, target))
        || window_frame_bound_references_group_output(&clause.spec.frame.start_bound, target)
        || window_frame_bound_references_group_output(&clause.spec.frame.end_bound, target)
}

fn window_func_references_group_output(func: &WindowFuncExpr, target: &Expr) -> bool {
    func.args
        .iter()
        .any(|expr| expr_references_group_output(expr, target))
        || match &func.kind {
            WindowFuncKind::Aggregate(aggref) => {
                aggref
                    .args
                    .iter()
                    .any(|expr| expr_references_group_output(expr, target))
                    || aggref
                        .aggorder
                        .iter()
                        .any(|item| expr_references_group_output(&item.expr, target))
                    || aggref
                        .aggfilter
                        .as_ref()
                        .is_some_and(|expr| expr_references_group_output(expr, target))
            }
            WindowFuncKind::Builtin(_) => false,
        }
}

fn window_frame_bound_references_group_output(bound: &WindowFrameBound, target: &Expr) -> bool {
    match bound {
        WindowFrameBound::OffsetPreceding(offset) | WindowFrameBound::OffsetFollowing(offset) => {
            expr_references_group_output(&offset.expr, target)
        }
        WindowFrameBound::UnboundedPreceding
        | WindowFrameBound::CurrentRow
        | WindowFrameBound::UnboundedFollowing => false,
    }
}

fn expr_references_group_output(expr: &Expr, target: &Expr) -> bool {
    if expr == target {
        return true;
    }

    match expr {
        // Aggregate inputs are evaluated below the aggregate node, so they do not
        // need extra passthrough slots in the grouped output.
        Expr::Aggref(_) => false,
        Expr::GroupingKey(grouping_key) => expr_references_group_output(&grouping_key.expr, target),
        Expr::GroupingFunc(grouping_func) => grouping_func
            .args
            .iter()
            .any(|arg| expr_references_group_output(arg, target)),
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
        | Expr::LocalTimestamp { .. } => false,
        Expr::WindowFunc(window_func) => window_func_references_group_output(window_func, target),
        Expr::Op(op) => op
            .args
            .iter()
            .any(|arg| expr_references_group_output(arg, target)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(|arg| expr_references_group_output(arg, target)),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_deref()
                .is_some_and(|arg| expr_references_group_output(arg, target))
                || case_expr.args.iter().any(|arm| {
                    expr_references_group_output(&arm.expr, target)
                        || expr_references_group_output(&arm.result, target)
                })
                || expr_references_group_output(&case_expr.defresult, target)
        }
        Expr::Func(func) => func
            .args
            .iter()
            .any(|arg| expr_references_group_output(arg, target)),
        Expr::SqlJsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
            .any(|arg| expr_references_group_output(arg, target)),
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .any(|arg| expr_references_group_output(arg, target)),
        Expr::SubLink(sublink) => sublink
            .testexpr
            .as_deref()
            .is_some_and(|expr| expr_references_group_output(expr, target)),
        Expr::SubPlan(subplan) => {
            subplan
                .testexpr
                .as_deref()
                .is_some_and(|expr| expr_references_group_output(expr, target))
                || subplan
                    .args
                    .iter()
                    .any(|arg| expr_references_group_output(arg, target))
        }
        Expr::ScalarArrayOp(op) => {
            expr_references_group_output(&op.left, target)
                || expr_references_group_output(&op.right, target)
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .any(|child| expr_references_group_output(child, target)),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => expr_references_group_output(inner, target),
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
            expr_references_group_output(expr, target)
                || expr_references_group_output(pattern, target)
                || escape
                    .as_deref()
                    .is_some_and(|expr| expr_references_group_output(expr, target))
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_references_group_output(left, target)
                || expr_references_group_output(right, target)
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .any(|expr| expr_references_group_output(expr, target)),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_references_group_output(expr, target)),
        Expr::FieldSelect { expr, .. } => expr_references_group_output(expr, target),
        Expr::ArraySubscript { array, subscripts } => {
            expr_references_group_output(array, target)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(|expr| expr_references_group_output(expr, target))
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(|expr| expr_references_group_output(expr, target))
                })
        }
    }
}

fn remove_redundant_relation_group_keys(query: &Query, catalog: &dyn CatalogLookup) -> Vec<Expr> {
    let group_vars = query
        .group_by
        .iter()
        .map(|expr| analyze_group_var(query, expr))
        .collect::<Vec<_>>();

    let mut grouped_attnos = BTreeMap::<usize, BTreeSet<i16>>::new();
    for info in group_vars.iter().flatten() {
        grouped_attnos
            .entry(info.relid)
            .or_default()
            .insert(info.attno);
    }

    let mut surplus_attnos = BTreeMap::<usize, BTreeSet<i16>>::new();
    for (&relid, attnos) in &grouped_attnos {
        if attnos.len() < 2 {
            continue;
        }
        let Some(rte) = query.rtable.get(relid.saturating_sub(1)) else {
            continue;
        };
        let RangeTblEntryKind::Relation {
            relation_oid,
            relkind,
            ..
        } = &rte.kind
        else {
            continue;
        };
        if !matches!(*relkind, 'r' | 'p') {
            continue;
        }
        if *relkind != 'p' && rte.inh && catalog.find_all_inheritors(*relation_oid).len() > 1 {
            continue;
        }

        let Some(best_key) = best_unique_group_subset(catalog, *relation_oid, &rte.desc, attnos)
        else {
            continue;
        };
        if best_key.len() >= attnos.len() {
            continue;
        }
        let removable = attnos
            .iter()
            .copied()
            .filter(|attno| !best_key.contains(attno))
            .collect::<BTreeSet<_>>();
        if !removable.is_empty() {
            surplus_attnos.insert(relid, removable);
        }
    }

    query
        .group_by
        .iter()
        .cloned()
        .zip(group_vars)
        .filter_map(|(expr, info)| match info {
            Some(info)
                if surplus_attnos
                    .get(&info.relid)
                    .is_some_and(|attnos| attnos.contains(&info.attno)) =>
            {
                None
            }
            _ => Some(expr),
        })
        .collect()
}

fn best_unique_group_subset(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    desc: &pgrust_nodes::primnodes::RelationDesc,
    grouped_attnos: &BTreeSet<i16>,
) -> Option<BTreeSet<i16>> {
    let not_null_attnos = catalog
        .attribute_rows_for_relation(relation_oid)
        .into_iter()
        .filter_map(|attr| (attr.attnum > 0 && attr.attnotnull).then_some(attr.attnum))
        .collect::<BTreeSet<_>>();
    let mut best_key: Option<BTreeSet<i16>> = None;
    for index in catalog.index_relations_for_heap(relation_oid) {
        let meta = &index.index_meta;
        if !meta.indisunique || !meta.indisvalid || !meta.indisready || !meta.indimmediate {
            continue;
        }
        if !index.index_exprs.is_empty() || index.index_predicate.is_some() {
            continue;
        }
        let key_attnos = meta
            .indkey
            .iter()
            .take(meta.indnkeyatts as usize)
            .copied()
            .collect::<Vec<_>>();
        if key_attnos.is_empty() || key_attnos.iter().any(|attno| *attno <= 0) {
            continue;
        }
        if !meta.indisprimary
            && !meta.indnullsnotdistinct
            && key_attnos
                .iter()
                .any(|attno| !column_known_not_null(desc, &not_null_attnos, *attno))
        {
            continue;
        }

        let key_attnos = key_attnos.into_iter().collect::<BTreeSet<_>>();
        if key_attnos.len() >= grouped_attnos.len() || !key_attnos.is_subset(grouped_attnos) {
            continue;
        }
        match &best_key {
            Some(existing) if existing.len() <= key_attnos.len() => {}
            _ => best_key = Some(key_attnos),
        }
    }
    best_key
}

fn column_known_not_null(
    desc: &pgrust_nodes::primnodes::RelationDesc,
    not_null_attnos: &BTreeSet<i16>,
    attno: i16,
) -> bool {
    not_null_attnos.contains(&attno)
        || desc
            .columns
            .get((attno - 1) as usize)
            .is_some_and(|column| !column.storage.nullable)
}

fn collapse_duplicate_group_keys(query: &Query, group_by: Vec<Expr>) -> Vec<Expr> {
    if group_by.len() < 2 {
        return group_by;
    }

    let mut group_keys = Vec::with_capacity(group_by.len());
    let mut positions_by_key = BTreeMap::<VarKey, Vec<usize>>::new();
    for (index, expr) in group_by.iter().enumerate() {
        let flattened = flatten_join_alias_vars_query(query, expr.clone());
        let key = flattened_var_key(&flattened);
        if let Some(key) = key {
            positions_by_key.entry(key).or_default().push(index);
        }
        group_keys.push(key);
    }

    let mut parent = (0..group_by.len()).collect::<Vec<_>>();
    for (left_key, right_key) in collect_inner_join_equality_pairs(query) {
        let Some(left_positions) = positions_by_key.get(&left_key) else {
            continue;
        };
        let Some(right_positions) = positions_by_key.get(&right_key) else {
            continue;
        };
        for &left in left_positions {
            for &right in right_positions {
                union_roots(&mut parent, left, right);
            }
        }
    }

    let mut seen_roots = BTreeSet::new();
    group_by
        .into_iter()
        .enumerate()
        .filter_map(|(index, expr)| {
            let root = find_root(&mut parent, index);
            seen_roots.insert(root).then_some(expr)
        })
        .collect()
}

fn collect_inner_join_equality_pairs(query: &Query) -> Vec<(VarKey, VarKey)> {
    let mut pairs = Vec::new();
    if let Some(jointree) = query.jointree.as_ref() {
        collect_inner_join_pairs_from_tree(query, jointree, &mut pairs);
    }
    pairs
}

fn collect_inner_join_pairs_from_tree(
    query: &Query,
    node: &JoinTreeNode,
    pairs: &mut Vec<(VarKey, VarKey)>,
) {
    match node {
        JoinTreeNode::RangeTblRef(_) => {}
        JoinTreeNode::JoinExpr {
            left,
            right,
            kind,
            quals,
            ..
        } => {
            collect_inner_join_pairs_from_tree(query, left, pairs);
            collect_inner_join_pairs_from_tree(query, right, pairs);
            if matches!(kind, JoinType::Inner | JoinType::Cross) {
                collect_pairs_from_qual(query, quals, pairs);
            }
        }
    }
}

fn collect_pairs_from_qual(query: &Query, expr: &Expr, pairs: &mut Vec<(VarKey, VarKey)>) {
    match expr {
        Expr::Bool(bool_expr)
            if matches!(bool_expr.boolop, pgrust_nodes::primnodes::BoolExprType::And) =>
        {
            for arg in &bool_expr.args {
                collect_pairs_from_qual(query, arg, pairs);
            }
        }
        Expr::Op(op) if op.op == OpExprKind::Eq && op.args.len() == 2 => {
            let left = flatten_join_alias_vars_query(query, op.args[0].clone());
            let right = flatten_join_alias_vars_query(query, op.args[1].clone());
            let (Some(left), Some(right)) = (flattened_var_key(&left), flattened_var_key(&right))
            else {
                return;
            };
            if left.varno != right.varno {
                pairs.push((left, right));
            }
        }
        _ => {}
    }
}

fn analyze_group_var(query: &Query, expr: &Expr) -> Option<GroupVarInfo> {
    let flattened = flatten_join_alias_vars_query(query, expr.clone());
    let Expr::Var(var) = &flattened else {
        return None;
    };
    if var.varlevelsup != 0 || var.varattno <= 0 {
        return None;
    }
    let attno = i16::try_from(var.varattno).ok()?;
    let rte = query.rtable.get(var.varno.saturating_sub(1))?;
    matches!(rte.kind, RangeTblEntryKind::Relation { .. }).then_some(GroupVarInfo {
        relid: var.varno,
        attno,
    })
}

fn flattened_var_key(expr: &Expr) -> Option<VarKey> {
    let Expr::Var(Var {
        varno,
        varattno,
        varlevelsup,
        ..
    }) = expr
    else {
        return None;
    };
    (*varattno > 0).then_some(VarKey {
        varno: *varno,
        varattno: *varattno,
        varlevelsup: *varlevelsup,
    })
}

fn query_has_outer_joins(query: &Query) -> bool {
    fn jointree_has_outer_join(node: &JoinTreeNode) -> bool {
        match node {
            JoinTreeNode::RangeTblRef(_) => false,
            JoinTreeNode::JoinExpr {
                left, right, kind, ..
            } => {
                !matches!(kind, JoinType::Inner | JoinType::Cross)
                    || jointree_has_outer_join(left)
                    || jointree_has_outer_join(right)
            }
        }
    }

    query.jointree.as_ref().is_some_and(jointree_has_outer_join)
}

fn find_root(parent: &mut [usize], index: usize) -> usize {
    if parent[index] == index {
        return index;
    }
    let root = find_root(parent, parent[index]);
    parent[index] = root;
    root
}

fn union_roots(parent: &mut [usize], left: usize, right: usize) {
    let left_root = find_root(parent, left);
    let right_root = find_root(parent, right);
    if left_root == right_root {
        return;
    }
    if left_root < right_root {
        parent[right_root] = left_root;
    } else {
        parent[left_root] = right_root;
    }
}
