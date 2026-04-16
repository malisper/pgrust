use crate::include::nodes::parsenodes::{JoinTreeNode, Query, RangeTblEntry, RangeTblEntryKind};
use crate::include::nodes::pathnodes::{PathTarget, PlannerInfo, RelOptInfo};
use crate::include::nodes::primnodes::{
    Aggref, Expr, ProjectSetTarget, SetReturningCall, TargetEntry, Var,
};

use super::joininfo::build_special_join_info;
use super::pathnodes::expr_sql_type;

impl PlannerInfo {
    pub fn new(parse: Query) -> Self {
        let processed_tlist = make_processed_tlist(&parse);
        let final_target = PathTarget::from_target_list(&parse.target_list);
        let query_pathkeys = PathTarget::from_sort_clause(&parse.sort_clause, &processed_tlist);
        let sort_input_target = make_sort_input_target(&parse, &processed_tlist, &final_target);
        let group_input_target = if has_grouping(&parse) {
            make_group_input_target(&parse)
        } else {
            sort_input_target.clone()
        };
        let grouped_target = if has_grouping(&parse) {
            build_grouped_target(&parse)
        } else {
            final_target.clone()
        };
        let scanjoin_target = build_scanjoin_target(
            &parse,
            &group_input_target,
            &sort_input_target,
            &final_target,
        );
        let simple_rel_array = build_simple_rel_array(&parse.rtable);
        let join_info_list = build_special_join_info(&parse);
        Self {
            processed_tlist,
            scanjoin_target,
            group_input_target,
            grouped_target,
            sort_input_target,
            final_target,
            query_pathkeys,
            simple_rel_array,
            append_rel_infos: vec![None; parse.rtable.len() + 1],
            join_rel_list: Vec::new(),
            upper_rels: Vec::new(),
            join_info_list,
            inner_join_clauses: Vec::new(),
            final_rel: None,
            parse,
        }
    }
}

pub(super) fn build_projection_targets_for_pathtarget(target: &PathTarget) -> Vec<TargetEntry> {
    target
        .exprs
        .iter()
        .enumerate()
        .map(|(index, expr)| {
            TargetEntry::new(
                format!("col{}", index + 1),
                expr.clone(),
                expr_sql_type(expr),
                index + 1,
            )
            .with_sort_group_ref(target.get_pathtarget_sortgroupref(index))
        })
        .collect()
}

pub(super) fn build_simple_rel_array(rtable: &[RangeTblEntry]) -> Vec<Option<RelOptInfo>> {
    let mut simple_rel_array = vec![None];
    simple_rel_array.extend(
        rtable
            .iter()
            .enumerate()
            .map(|(index, rte)| match &rte.kind {
                RangeTblEntryKind::Join { .. } => None,
                _ => Some(RelOptInfo::from_rte(index + 1, rte)),
            }),
    );
    simple_rel_array
}

fn has_grouping(query: &Query) -> bool {
    !query.group_by.is_empty() || !query.accumulators.is_empty() || query.having_qual.is_some()
}

fn make_processed_tlist(parse: &Query) -> Vec<TargetEntry> {
    let mut processed_tlist = parse.target_list.clone();
    let mut next_sort_group_ref = processed_tlist
        .iter()
        .map(|target| target.ressortgroupref)
        .max()
        .unwrap_or(0)
        + 1;
    let mut next_resno = processed_tlist.len() + 1;

    for clause in &parse.sort_clause {
        if let Some(target) = processed_tlist
            .iter_mut()
            .find(|target| target.expr == clause.expr)
        {
            if target.ressortgroupref == 0 {
                target.ressortgroupref = next_sort_group_ref;
                next_sort_group_ref += 1;
            }
            continue;
        }

        processed_tlist.push(
            TargetEntry::new(
                "?column?",
                clause.expr.clone(),
                expr_sql_type(&clause.expr),
                next_resno,
            )
            .with_sort_group_ref(next_sort_group_ref)
            .as_resjunk(),
        );
        next_sort_group_ref += 1;
        next_resno += 1;
    }

    processed_tlist
}

pub(super) fn project_set_base_width(project_set: &[ProjectSetTarget]) -> usize {
    project_set
        .iter()
        .take_while(|target| matches!(target, ProjectSetTarget::Scalar(_)))
        .count()
}

pub(super) fn target_references_project_set_output(target: &TargetEntry, base_width: usize) -> bool {
    target
        .input_resno
        .is_some_and(|input_resno| input_resno > base_width)
}

fn collect_set_returning_call_supporting_inputs(call: &SetReturningCall, exprs: &mut Vec<Expr>) {
    match call {
        SetReturningCall::GenerateSeries {
            start, stop, step, ..
        } => {
            collect_supporting_inputs(start, exprs);
            collect_supporting_inputs(stop, exprs);
            collect_supporting_inputs(step, exprs);
        }
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. }
        | SetReturningCall::UserDefined { args, .. } => {
            for arg in args {
                collect_supporting_inputs(arg, exprs);
            }
        }
    }
}

fn make_sort_input_target(
    parse: &Query,
    processed_tlist: &[TargetEntry],
    final_target: &PathTarget,
) -> PathTarget {
    if parse.sort_clause.is_empty() {
        return final_target.clone();
    }

    let Some(project_set) = parse.project_set.as_ref() else {
        return PathTarget::from_target_list(processed_tlist);
    };

    let base_width = project_set_base_width(project_set);
    let have_srf_sortcols = processed_tlist.iter().any(|target| {
        target.ressortgroupref != 0 && target_references_project_set_output(target, base_width)
    });
    if have_srf_sortcols {
        return PathTarget::from_target_list(processed_tlist);
    }

    let mut input_target = PathTarget::new(Vec::new());
    for target in processed_tlist {
        if target_references_project_set_output(target, base_width) {
            continue;
        }
        input_target.add_column_to_pathtarget(target.expr.clone(), target.ressortgroupref);
    }
    for target in project_set {
        if let ProjectSetTarget::Set { call, .. } = target {
            let mut supporting_inputs = Vec::new();
            collect_set_returning_call_supporting_inputs(call, &mut supporting_inputs);
            input_target.add_new_columns_to_pathtarget(supporting_inputs);
        }
    }
    input_target
}

fn make_group_input_target(parse: &Query) -> PathTarget {
    let mut exprs = Vec::new();
    for group_expr in &parse.group_by {
        push_expr(&mut exprs, group_expr.clone());
    }
    for target in &parse.target_list {
        collect_group_input_exprs(&target.expr, &parse.group_by, &mut exprs);
    }
    for accum in &parse.accumulators {
        for arg in &accum.args {
            collect_group_input_exprs(arg, &parse.group_by, &mut exprs);
        }
    }
    if let Some(having) = parse.having_qual.as_ref() {
        collect_group_input_exprs(having, &parse.group_by, &mut exprs);
    }
    if let Some(where_qual) = parse.where_qual.as_ref() {
        collect_group_input_exprs(where_qual, &parse.group_by, &mut exprs);
    }
    PathTarget::new(exprs)
}

fn build_grouped_target(parse: &Query) -> PathTarget {
    let mut exprs = parse.group_by.clone();
    exprs.extend(parse.accumulators.iter().enumerate().map(|(aggno, accum)| {
        Expr::Aggref(Box::new(Aggref {
            aggfnoid: accum.aggfnoid,
            aggtype: accum.sql_type,
            aggvariadic: accum.agg_variadic,
            aggdistinct: accum.distinct,
            args: accum.args.clone(),
            agglevelsup: 0,
            aggno,
        }))
    }));
    PathTarget::new(exprs)
}

fn build_scanjoin_target(
    parse: &Query,
    group_input_target: &PathTarget,
    sort_input_target: &PathTarget,
    final_target: &PathTarget,
) -> PathTarget {
    let exprs = if has_grouping(parse) {
        group_input_target.exprs.clone()
    } else if !parse.sort_clause.is_empty() {
        sort_input_target.exprs.clone()
    } else {
        final_target.exprs.clone()
    };
    PathTarget::new(exprs)
}

fn push_expr(exprs: &mut Vec<Expr>, expr: Expr) {
    if !exprs.contains(&expr) {
        exprs.push(expr);
    }
}

fn collect_group_input_exprs(expr: &Expr, group_by: &[Expr], exprs: &mut Vec<Expr>) {
    if group_by.contains(expr) {
        push_expr(exprs, expr.clone());
        return;
    }
    match expr {
        Expr::Var(_) | Expr::OuterColumn { .. } => push_expr(exprs, expr.clone()),
        Expr::Param(_) => {}
        Expr::Column(_) => panic!("unexpected Expr::Column in group input target derivation"),
        Expr::Aggref(aggref) => {
            for arg in &aggref.args {
                collect_group_input_exprs(arg, group_by, exprs);
            }
        }
        Expr::Op(op) => collect_expr_vec(&op.args, group_by, exprs),
        Expr::Bool(bool_expr) => collect_expr_vec(&bool_expr.args, group_by, exprs),
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_group_input_exprs(arg, group_by, exprs);
            }
            for arm in &case_expr.args {
                collect_group_input_exprs(&arm.expr, group_by, exprs);
                collect_group_input_exprs(&arm.result, group_by, exprs);
            }
            collect_group_input_exprs(&case_expr.defresult, group_by, exprs);
        }
        Expr::CaseTest(_) => {}
        Expr::Func(func) => collect_expr_vec(&func.args, group_by, exprs),
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_group_input_exprs(testexpr, group_by, exprs);
            }
            collect_query_outer_refs(&sublink.subselect, 1, exprs);
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                collect_group_input_exprs(testexpr, group_by, exprs);
            }
        }
        Expr::ScalarArrayOp(saop) => {
            collect_group_input_exprs(&saop.left, group_by, exprs);
            collect_group_input_exprs(&saop.right, group_by, exprs);
        }
        Expr::Cast(inner, _) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_group_input_exprs(inner, group_by, exprs);
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
            collect_group_input_exprs(expr, group_by, exprs);
            collect_group_input_exprs(pattern, group_by, exprs);
            if let Some(escape) = escape.as_deref() {
                collect_group_input_exprs(escape, group_by, exprs);
            }
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_group_input_exprs(left, group_by, exprs);
            collect_group_input_exprs(right, group_by, exprs);
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_group_input_exprs(element, group_by, exprs);
            }
        }
        Expr::ArraySubscript { array, subscripts } => {
            collect_group_input_exprs(array, group_by, exprs);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_group_input_exprs(lower, group_by, exprs);
                }
                if let Some(upper) = &subscript.upper {
                    collect_group_input_exprs(upper, group_by, exprs);
                }
            }
        }
        Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => {}
    }
}

fn collect_expr_vec(args: &[Expr], group_by: &[Expr], exprs: &mut Vec<Expr>) {
    for arg in args {
        collect_group_input_exprs(arg, group_by, exprs);
    }
}

fn collect_supporting_inputs(expr: &Expr, exprs: &mut Vec<Expr>) {
    match expr {
        Expr::Var(_) | Expr::OuterColumn { .. } => push_expr(exprs, expr.clone()),
        Expr::Param(_) => {}
        Expr::Column(_) => panic!("unexpected Expr::Column in supporting input derivation"),
        Expr::Aggref(aggref) => {
            for arg in &aggref.args {
                collect_supporting_inputs(arg, exprs);
            }
        }
        Expr::Op(op) => {
            for arg in &op.args {
                collect_supporting_inputs(arg, exprs);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &bool_expr.args {
                collect_supporting_inputs(arg, exprs);
            }
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_supporting_inputs(arg, exprs);
            }
            for arm in &case_expr.args {
                collect_supporting_inputs(&arm.expr, exprs);
                collect_supporting_inputs(&arm.result, exprs);
            }
            collect_supporting_inputs(&case_expr.defresult, exprs);
        }
        Expr::CaseTest(_) => {}
        Expr::Func(func) => {
            for arg in &func.args {
                collect_supporting_inputs(arg, exprs);
            }
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_supporting_inputs(testexpr, exprs);
            }
            collect_query_outer_refs(&sublink.subselect, 1, exprs);
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                collect_supporting_inputs(testexpr, exprs);
            }
        }
        Expr::ScalarArrayOp(saop) => {
            collect_supporting_inputs(&saop.left, exprs);
            collect_supporting_inputs(&saop.right, exprs);
        }
        Expr::Cast(inner, _) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_supporting_inputs(inner, exprs);
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
            collect_supporting_inputs(expr, exprs);
            collect_supporting_inputs(pattern, exprs);
            if let Some(escape) = escape.as_deref() {
                collect_supporting_inputs(escape, exprs);
            }
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_supporting_inputs(left, exprs);
            collect_supporting_inputs(right, exprs);
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_supporting_inputs(element, exprs);
            }
        }
        Expr::ArraySubscript { array, subscripts } => {
            collect_supporting_inputs(array, exprs);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_supporting_inputs(lower, exprs);
                }
                if let Some(upper) = &subscript.upper {
                    collect_supporting_inputs(upper, exprs);
                }
            }
        }
        Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => {}
    }
}

fn collect_query_outer_refs(query: &Query, levelsup: usize, exprs: &mut Vec<Expr>) {
    for target in &query.target_list {
        collect_query_outer_refs_expr(&target.expr, levelsup, exprs);
    }
    if let Some(where_qual) = query.where_qual.as_ref() {
        collect_query_outer_refs_expr(where_qual, levelsup, exprs);
    }
    for expr in &query.group_by {
        collect_query_outer_refs_expr(expr, levelsup, exprs);
    }
    for accum in &query.accumulators {
        for arg in &accum.args {
            collect_query_outer_refs_expr(arg, levelsup, exprs);
        }
    }
    if let Some(having) = query.having_qual.as_ref() {
        collect_query_outer_refs_expr(having, levelsup, exprs);
    }
    for clause in &query.sort_clause {
        collect_query_outer_refs_expr(&clause.expr, levelsup, exprs);
    }
    if let Some(project_set) = query.project_set.as_ref() {
        for target in project_set {
            collect_project_set_outer_refs(target, levelsup, exprs);
        }
    }
    if let Some(jointree) = query.jointree.as_ref() {
        collect_jointree_outer_refs(jointree, levelsup, exprs);
    }
    for rte in &query.rtable {
        match &rte.kind {
            RangeTblEntryKind::Values { rows, .. } => {
                for row in rows {
                    for expr in row {
                        collect_query_outer_refs_expr(expr, levelsup, exprs);
                    }
                }
            }
            RangeTblEntryKind::Function { call } => {
                collect_set_returning_call_outer_refs(call, levelsup, exprs)
            }
            RangeTblEntryKind::Cte { query, .. } => {
                collect_query_outer_refs(query, levelsup + 1, exprs)
            }
            RangeTblEntryKind::Subquery { query } => {
                collect_query_outer_refs(query, levelsup + 1, exprs)
            }
            RangeTblEntryKind::WorkTable { .. } => {}
            RangeTblEntryKind::Result
            | RangeTblEntryKind::Relation { .. }
            | RangeTblEntryKind::Join { .. } => {}
        }
    }
}

fn collect_jointree_supporting_inputs(node: &JoinTreeNode, exprs: &mut Vec<Expr>) {
    if let JoinTreeNode::JoinExpr {
        left, right, quals, ..
    } = node
    {
        collect_jointree_supporting_inputs(left, exprs);
        collect_jointree_supporting_inputs(right, exprs);
        collect_supporting_inputs(quals, exprs);
    }
}

fn collect_jointree_outer_refs(node: &JoinTreeNode, levelsup: usize, exprs: &mut Vec<Expr>) {
    match node {
        JoinTreeNode::RangeTblRef(_) => {}
        JoinTreeNode::JoinExpr {
            left, right, quals, ..
        } => {
            collect_jointree_outer_refs(left, levelsup, exprs);
            collect_jointree_outer_refs(right, levelsup, exprs);
            collect_query_outer_refs_expr(quals, levelsup, exprs);
        }
    }
}

fn collect_project_set_outer_refs(
    target: &ProjectSetTarget,
    levelsup: usize,
    exprs: &mut Vec<Expr>,
) {
    match target {
        ProjectSetTarget::Scalar(entry) => {
            collect_query_outer_refs_expr(&entry.expr, levelsup, exprs)
        }
        ProjectSetTarget::Set { call, .. } => {
            collect_set_returning_call_outer_refs(call, levelsup, exprs)
        }
    }
}

fn collect_set_returning_call_outer_refs(
    call: &SetReturningCall,
    levelsup: usize,
    exprs: &mut Vec<Expr>,
) {
    match call {
        SetReturningCall::GenerateSeries {
            start, stop, step, ..
        } => {
            collect_query_outer_refs_expr(start, levelsup, exprs);
            collect_query_outer_refs_expr(stop, levelsup, exprs);
            collect_query_outer_refs_expr(step, levelsup, exprs);
        }
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. }
        | SetReturningCall::UserDefined { args, .. } => {
            for arg in args {
                collect_query_outer_refs_expr(arg, levelsup, exprs);
            }
        }
    }
}

fn collect_query_outer_refs_expr(expr: &Expr, levelsup: usize, exprs: &mut Vec<Expr>) {
    match expr {
        Expr::Var(var) if var.varlevelsup == levelsup => push_expr(
            exprs,
            Expr::Var(Var {
                varlevelsup: 0,
                ..*var
            }),
        ),
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::OuterColumn { .. }
        | Expr::Column(_)
        | Expr::Const(_)
        | Expr::Random => {}
        Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => {}
        Expr::Aggref(aggref) => {
            for arg in &aggref.args {
                collect_query_outer_refs_expr(arg, levelsup, exprs);
            }
        }
        Expr::Op(op) => {
            for arg in &op.args {
                collect_query_outer_refs_expr(arg, levelsup, exprs);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &bool_expr.args {
                collect_query_outer_refs_expr(arg, levelsup, exprs);
            }
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_query_outer_refs_expr(arg, levelsup, exprs);
            }
            for arm in &case_expr.args {
                collect_query_outer_refs_expr(&arm.expr, levelsup, exprs);
                collect_query_outer_refs_expr(&arm.result, levelsup, exprs);
            }
            collect_query_outer_refs_expr(&case_expr.defresult, levelsup, exprs);
        }
        Expr::CaseTest(_) => {}
        Expr::Func(func) => {
            for arg in &func.args {
                collect_query_outer_refs_expr(arg, levelsup, exprs);
            }
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_query_outer_refs_expr(testexpr, levelsup, exprs);
            }
            collect_query_outer_refs(&sublink.subselect, levelsup + 1, exprs);
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                collect_query_outer_refs_expr(testexpr, levelsup, exprs);
            }
        }
        Expr::ScalarArrayOp(saop) => {
            collect_query_outer_refs_expr(&saop.left, levelsup, exprs);
            collect_query_outer_refs_expr(&saop.right, levelsup, exprs);
        }
        Expr::Cast(inner, _) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_query_outer_refs_expr(inner, levelsup, exprs);
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
            collect_query_outer_refs_expr(expr, levelsup, exprs);
            collect_query_outer_refs_expr(pattern, levelsup, exprs);
            if let Some(escape) = escape.as_deref() {
                collect_query_outer_refs_expr(escape, levelsup, exprs);
            }
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_query_outer_refs_expr(left, levelsup, exprs);
            collect_query_outer_refs_expr(right, levelsup, exprs);
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_query_outer_refs_expr(element, levelsup, exprs);
            }
        }
        Expr::ArraySubscript { array, subscripts } => {
            collect_query_outer_refs_expr(array, levelsup, exprs);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_query_outer_refs_expr(lower, levelsup, exprs);
                }
                if let Some(upper) = &subscript.upper {
                    collect_query_outer_refs_expr(upper, levelsup, exprs);
                }
            }
        }
    }
}
