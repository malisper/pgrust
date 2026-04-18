use crate::include::nodes::parsenodes::{JoinTreeNode, Query, RangeTblEntryKind};
use crate::include::nodes::pathnodes::{PlannerInfo, RestrictInfo, SpecialJoinInfo};
use crate::include::nodes::primnodes::{
    attrno_index, BoolExprType, Expr, ExprArraySubscript, JoinType,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AncestorSide {
    Left,
    Right,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AncestorJoin {
    rtindex: usize,
    jointype: JoinType,
    side: AncestorSide,
    lhs_strict: bool,
}

pub(super) fn make_restrict_info(clause: Expr) -> RestrictInfo {
    let required_relids = expr_relids(&clause);
    RestrictInfo::new(clause, required_relids)
}

pub(super) fn build_special_join_info(query: &Query) -> Vec<SpecialJoinInfo> {
    #[derive(Debug, Clone)]
    struct JoinTreeInfo {
        relids: Vec<usize>,
        inner_join_relids: Vec<usize>,
    }

    fn walk(
        query: &Query,
        node: &JoinTreeNode,
        joins: &mut Vec<SpecialJoinInfo>,
        ancestors: &[AncestorJoin],
    ) -> JoinTreeInfo {
        match node {
            JoinTreeNode::RangeTblRef(rtindex) => JoinTreeInfo {
                relids: vec![*rtindex],
                inner_join_relids: Vec::new(),
            },
            JoinTreeNode::JoinExpr {
                left,
                right,
                kind,
                rtindex,
                quals,
            } => {
                let left_relids = jointree_relids(left);
                let right_relids = jointree_relids(right);
                let original_quals = quals.clone();
                let expanded_quals = if matches!(kind, JoinType::Inner | JoinType::Cross) {
                    None
                } else {
                    Some(flatten_join_alias_vars_query(query, original_quals.clone()))
                };
                let clause_relids = expanded_quals.as_ref().map(expr_relids).unwrap_or_default();
                let strict_relids = expanded_quals
                    .as_ref()
                    .map(strict_relids)
                    .unwrap_or_default();
                let lhs_strict = relids_overlap(&strict_relids, &left_relids);
                let left_ancestors =
                    extend_ancestors(ancestors, *kind, *rtindex, AncestorSide::Left, lhs_strict);
                let right_ancestors =
                    extend_ancestors(ancestors, *kind, *rtindex, AncestorSide::Right, lhs_strict);
                let left_info = walk(query, left, joins, &left_ancestors);
                let right_info = walk(query, right, joins, &right_ancestors);
                let relids = relids_union(&left_relids, &right_relids);
                let inner_join_relids = if matches!(kind, JoinType::Inner | JoinType::Cross) {
                    relids_union(
                        &relids_union(&left_info.inner_join_relids, &right_info.inner_join_relids),
                        &relids,
                    )
                } else {
                    relids_union(&left_info.inner_join_relids, &right_info.inner_join_relids)
                };
                if !matches!(kind, JoinType::Inner | JoinType::Cross) {
                    let expanded_quals = expanded_quals.expect("outer join quals");
                    let mut min_lefthand = if matches!(kind, JoinType::Full) {
                        left_relids.clone()
                    } else {
                        relids_intersection(&clause_relids, &left_relids)
                    };
                    let mut min_righthand = if matches!(kind, JoinType::Full) {
                        right_relids.clone()
                    } else {
                        relids_intersection(
                            &relids_union(&clause_relids, &right_info.inner_join_relids),
                            &right_relids,
                        )
                    };

                    if !matches!(kind, JoinType::Full) {
                        for other in joins.iter() {
                            if relids_overlap(&left_relids, &other.syn_righthand)
                                && relids_overlap(&clause_relids, &other.syn_righthand)
                                && !relids_overlap(&strict_relids, &other.min_righthand)
                            {
                                min_lefthand = relids_union(&min_lefthand, &other.syn_lefthand);
                                min_lefthand = relids_union(&min_lefthand, &other.syn_righthand);
                            }

                            if relids_overlap(&right_relids, &other.syn_righthand)
                                && (relids_overlap(&clause_relids, &other.syn_righthand)
                                    || !relids_overlap(&clause_relids, &other.min_lefthand)
                                    || !other.lhs_strict)
                            {
                                min_righthand = relids_union(&min_righthand, &other.syn_lefthand);
                                min_righthand = relids_union(&min_righthand, &other.syn_righthand);
                            }
                        }
                    }

                    // PostgreSQL's make_outerjoininfo() never leaves either minimum-hand
                    // relset empty, even for clauses like ON TRUE. Later join-legal checks
                    // depend on both sides being populated so the outer join remains visible
                    // as a SpecialJoinInfo instead of degenerating into an inner join.
                    if min_lefthand.is_empty() {
                        min_lefthand = left_relids.clone();
                    }
                    if min_righthand.is_empty() {
                        min_righthand = right_relids.clone();
                    }

                    let (commute_above_l, commute_above_r) =
                        build_commute_above(ancestors, *kind, lhs_strict);
                    joins.push(SpecialJoinInfo {
                        jointype: *kind,
                        rtindex: *rtindex,
                        ojrelid: Some(*rtindex),
                        min_lefthand,
                        min_righthand,
                        syn_lefthand: left_relids.clone(),
                        syn_righthand: right_relids.clone(),
                        commute_above_l,
                        commute_above_r,
                        commute_below_l: Vec::new(),
                        commute_below_r: Vec::new(),
                        lhs_strict,
                        join_quals: original_quals,
                    });
                }
                JoinTreeInfo {
                    relids,
                    inner_join_relids,
                }
            }
        }
    }

    let mut joins = Vec::new();
    if let Some(jointree) = query.jointree.as_ref() {
        walk(query, jointree, &mut joins, &[]);
    }
    populate_commute_below(&mut joins);
    joins
}

fn extend_ancestors(
    ancestors: &[AncestorJoin],
    kind: JoinType,
    rtindex: usize,
    side: AncestorSide,
    lhs_strict: bool,
) -> Vec<AncestorJoin> {
    let mut next = ancestors.to_vec();
    if !matches!(kind, JoinType::Inner | JoinType::Cross) {
        next.push(AncestorJoin {
            rtindex,
            jointype: kind,
            side,
            lhs_strict,
        });
    }
    next
}

fn build_commute_above(
    ancestors: &[AncestorJoin],
    jointype: JoinType,
    lhs_strict: bool,
) -> (Vec<usize>, Vec<usize>) {
    if jointype != JoinType::Left || !lhs_strict {
        return (Vec::new(), Vec::new());
    }
    let mut commute_above_l = Vec::new();
    let mut commute_above_r = Vec::new();
    for ancestor in ancestors {
        if ancestor.jointype != JoinType::Left || !ancestor.lhs_strict {
            continue;
        }
        match ancestor.side {
            AncestorSide::Left => push_relid(&mut commute_above_l, ancestor.rtindex),
            AncestorSide::Right => push_relid(&mut commute_above_r, ancestor.rtindex),
        }
    }
    (commute_above_l, commute_above_r)
}

fn jointree_relids(node: &JoinTreeNode) -> Vec<usize> {
    match node {
        JoinTreeNode::RangeTblRef(rtindex) => vec![*rtindex],
        JoinTreeNode::JoinExpr { left, right, .. } => {
            relids_union(&jointree_relids(left), &jointree_relids(right))
        }
    }
}

fn populate_commute_below(joins: &mut [SpecialJoinInfo]) {
    let rtindexes = joins
        .iter()
        .enumerate()
        .map(|(index, sj)| (sj.rtindex, index))
        .collect::<std::collections::HashMap<_, _>>();
    let updates = joins
        .iter()
        .map(|sj| {
            (
                sj.rtindex,
                sj.commute_above_l.clone(),
                sj.commute_above_r.clone(),
            )
        })
        .collect::<Vec<_>>();
    for (rtindex, commute_above_l, commute_above_r) in updates {
        for ancestor_rtindex in commute_above_l {
            if let Some(index) = rtindexes.get(&ancestor_rtindex).copied() {
                push_relid(&mut joins[index].commute_below_l, rtindex);
            }
        }
        for ancestor_rtindex in commute_above_r {
            if let Some(index) = rtindexes.get(&ancestor_rtindex).copied() {
                push_relid(&mut joins[index].commute_below_r, rtindex);
            }
        }
    }
}

fn push_relid(relids: &mut Vec<usize>, relid: usize) {
    if !relids.contains(&relid) {
        relids.push(relid);
    }
}

pub(super) fn relids_union(left: &[usize], right: &[usize]) -> Vec<usize> {
    let mut relids = left.to_vec();
    relids.extend(right.iter().copied());
    relids.sort_unstable();
    relids.dedup();
    relids
}

pub(super) fn relids_intersection(left: &[usize], right: &[usize]) -> Vec<usize> {
    left.iter()
        .copied()
        .filter(|relid| right.contains(relid))
        .collect()
}

pub(super) fn relids_subset(required: &[usize], available: &[usize]) -> bool {
    required.iter().all(|relid| available.contains(relid))
}

pub(super) fn relids_overlap(left: &[usize], right: &[usize]) -> bool {
    left.iter().any(|relid| right.contains(relid))
}

pub(super) fn relids_disjoint(left: &[usize], right: &[usize]) -> bool {
    !relids_overlap(left, right)
}

pub(super) fn expand_join_rte_vars(root: &PlannerInfo, expr: Expr) -> Expr {
    flatten_join_alias_vars_query(&root.parse, expr)
}

pub(super) fn expand_join_rte_vars_query(query: &Query, expr: Expr) -> Expr {
    flatten_join_alias_vars_query(query, expr)
}

pub(super) fn flatten_join_alias_vars(root: &PlannerInfo, expr: Expr) -> Expr {
    flatten_join_alias_vars_query(&root.parse, expr)
}

pub(super) fn flatten_join_alias_vars_query(query: &Query, expr: Expr) -> Expr {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 => {
            let Some(rte) = query.rtable.get(var.varno.saturating_sub(1)) else {
                return Expr::Var(var);
            };
            let RangeTblEntryKind::Join { joinaliasvars, .. } = &rte.kind else {
                return Expr::Var(var);
            };
            joinaliasvars
                .get(attrno_index(var.varattno).unwrap_or(usize::MAX))
                .cloned()
                .map(|expr| flatten_join_alias_vars_query(query, expr))
                .unwrap_or(Expr::Var(var))
        }
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(crate::include::nodes::primnodes::Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| flatten_join_alias_vars_query(query, arg))
                .collect(),
            aggorder: aggref
                .aggorder
                .into_iter()
                .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                    expr: flatten_join_alias_vars_query(query, item.expr),
                    ..item
                })
                .collect(),
            aggfilter: aggref
                .aggfilter
                .map(|expr| flatten_join_alias_vars_query(query, expr)),
            ..*aggref
        })),
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| flatten_join_alias_vars_query(query, arg))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| flatten_join_alias_vars_query(query, arg))
                .collect(),
            ..*bool_expr
        })),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|expr| Box::new(flatten_join_alias_vars_query(query, *expr))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: flatten_join_alias_vars_query(query, arm.expr),
                    result: flatten_join_alias_vars_query(query, arm.result),
                })
                .collect(),
            defresult: Box::new(flatten_join_alias_vars_query(query, *case_expr.defresult)),
            ..*case_expr
        })),
        Expr::CaseTest(case_test) => Expr::CaseTest(case_test),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| flatten_join_alias_vars_query(query, arg))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => {
            Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
                testexpr: sublink
                    .testexpr
                    .map(|expr| Box::new(flatten_join_alias_vars_query(query, *expr))),
                ..*sublink
            }))
        }
        Expr::SubPlan(subplan) => {
            Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan
                    .testexpr
                    .map(|expr| Box::new(flatten_join_alias_vars_query(query, *expr))),
                ..*subplan
            }))
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(flatten_join_alias_vars_query(query, *saop.left)),
                right: Box::new(flatten_join_alias_vars_query(query, *saop.right)),
                ..*saop
            },
        )),
        Expr::Cast(inner, ty) => {
            Expr::Cast(Box::new(flatten_join_alias_vars_query(query, *inner)), ty)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(flatten_join_alias_vars_query(query, *expr)),
            pattern: Box::new(flatten_join_alias_vars_query(query, *pattern)),
            escape: escape.map(|expr| Box::new(flatten_join_alias_vars_query(query, *expr))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(flatten_join_alias_vars_query(query, *expr)),
            pattern: Box::new(flatten_join_alias_vars_query(query, *pattern)),
            escape: escape.map(|expr| Box::new(flatten_join_alias_vars_query(query, *expr))),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(flatten_join_alias_vars_query(query, *inner))),
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(flatten_join_alias_vars_query(query, *inner)))
        }
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(flatten_join_alias_vars_query(query, *left)),
            Box::new(flatten_join_alias_vars_query(query, *right)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(flatten_join_alias_vars_query(query, *left)),
            Box::new(flatten_join_alias_vars_query(query, *right)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| flatten_join_alias_vars_query(query, element))
                .collect(),
            array_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(flatten_join_alias_vars_query(query, *left)),
            Box::new(flatten_join_alias_vars_query(query, *right)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(flatten_join_alias_vars_query(query, *array)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| flatten_join_alias_vars_query(query, expr)),
                    upper: subscript
                        .upper
                        .map(|expr| flatten_join_alias_vars_query(query, expr)),
                })
                .collect(),
        },
        other => other,
    }
}

pub(super) fn strict_relids(expr: &Expr) -> Vec<usize> {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 => vec![var.varno],
        Expr::Aggref(aggref) => {
            let mut relids = strict_relids_union(&aggref.args);
            if let Some(filter) = aggref.aggfilter.as_ref() {
                relids = relids_union(&relids, &strict_relids(filter));
            }
            relids
        }
        Expr::Op(op) => strict_relids_union(&op.args),
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            BoolExprType::And => strict_relids_union(&bool_expr.args),
            BoolExprType::Or => {
                let mut iter = bool_expr.args.iter();
                let Some(first) = iter.next() else {
                    return Vec::new();
                };
                iter.fold(strict_relids(first), |acc, arg| {
                    relids_intersection(&acc, &strict_relids(arg))
                })
            }
            BoolExprType::Not => bool_expr
                .args
                .first()
                .map(strict_relids)
                .unwrap_or_default(),
        },
        Expr::Func(func) => strict_relids_union(&func.args),
        Expr::Case(case_expr) => {
            let mut relids = case_expr
                .arg
                .as_deref()
                .map(strict_relids)
                .unwrap_or_default();
            for arm in &case_expr.args {
                relids = relids_union(&relids, &strict_relids(&arm.expr));
                relids = relids_union(&relids, &strict_relids(&arm.result));
            }
            relids_union(&relids, &strict_relids(&case_expr.defresult))
        }
        Expr::CaseTest(_) => Vec::new(),
        Expr::ScalarArrayOp(saop) => {
            relids_union(&strict_relids(&saop.left), &strict_relids(&saop.right))
        }
        Expr::Cast(inner, _) => strict_relids(inner),
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
            let mut relids = relids_union(&strict_relids(expr), &strict_relids(pattern));
            if let Some(escape) = escape.as_deref() {
                relids = relids_union(&relids, &strict_relids(escape));
            }
            relids
        }
        Expr::ArraySubscript { array, subscripts } => {
            let mut relids = strict_relids(array);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    relids = relids_union(&relids, &strict_relids(lower));
                }
                if let Some(upper) = &subscript.upper {
                    relids = relids_union(&relids, &strict_relids(upper));
                }
            }
            relids
        }
        _ => Vec::new(),
    }
}

fn strict_relids_union(args: &[Expr]) -> Vec<usize> {
    args.iter().fold(Vec::new(), |acc, arg| {
        relids_union(&acc, &strict_relids(arg))
    })
}

pub(super) fn expr_relids(expr: &Expr) -> Vec<usize> {
    let mut relids = Vec::new();
    collect_expr_relids(expr, &mut relids);
    relids.sort_unstable();
    relids.dedup();
    relids
}

fn collect_expr_relids(expr: &Expr, relids: &mut Vec<usize>) {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 => relids.push(var.varno),
        Expr::Aggref(aggref) => {
            for arg in &aggref.args {
                collect_expr_relids(arg, relids);
            }
            if let Some(filter) = aggref.aggfilter.as_ref() {
                collect_expr_relids(filter, relids);
            }
        }
        Expr::WindowFunc(window_func) => {
            for arg in &window_func.args {
                collect_expr_relids(arg, relids);
            }
            if let crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) =
                &window_func.kind
            {
                if let Some(filter) = aggref.aggfilter.as_ref() {
                    collect_expr_relids(filter, relids);
                }
            }
        }
        Expr::Op(op) => {
            for arg in &op.args {
                collect_expr_relids(arg, relids);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &bool_expr.args {
                collect_expr_relids(arg, relids);
            }
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_expr_relids(arg, relids);
            }
            for arm in &case_expr.args {
                collect_expr_relids(&arm.expr, relids);
                collect_expr_relids(&arm.result, relids);
            }
            collect_expr_relids(&case_expr.defresult, relids);
        }
        Expr::CaseTest(_) => {}
        Expr::Func(func) => {
            for arg in &func.args {
                collect_expr_relids(arg, relids);
            }
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_expr_relids(testexpr, relids);
            }
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                collect_expr_relids(testexpr, relids);
            }
        }
        Expr::ScalarArrayOp(saop) => {
            collect_expr_relids(&saop.left, relids);
            collect_expr_relids(&saop.right, relids);
        }
        Expr::Cast(inner, _) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_expr_relids(inner, relids)
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
            collect_expr_relids(expr, relids);
            collect_expr_relids(pattern, relids);
            if let Some(escape) = escape {
                collect_expr_relids(escape, relids);
            }
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_expr_relids(left, relids);
            collect_expr_relids(right, relids);
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_expr_relids(element, relids);
            }
        }
        Expr::ArraySubscript { array, subscripts } => {
            collect_expr_relids(array, relids);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_expr_relids(lower, relids);
                }
                if let Some(upper) = &subscript.upper {
                    collect_expr_relids(upper, relids);
                }
            }
        }
        Expr::Row { fields, .. } => {
            for (_, expr) in fields {
                collect_expr_relids(expr, relids);
            }
        }
        Expr::Param(_)
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => {}
        Expr::Var(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::build_special_join_info;
    use crate::backend::parser::SqlType;
    use crate::backend::parser::SqlTypeKind;
    use crate::include::executor::execdesc::CommandType;
    use crate::include::nodes::parsenodes::{
        JoinTreeNode, Query, RangeTblEntry, RangeTblEntryKind,
    };
    use crate::include::nodes::primnodes::{
        Expr, JoinType, OpExpr, OpExprKind, RelationDesc, TargetEntry, Var,
    };

    fn query_for_jointree(jointree: JoinTreeNode, rtable: Vec<RangeTblEntry>) -> Query {
        Query {
            command_type: CommandType::Select,
            rtable,
            jointree: Some(jointree),
            target_list: vec![TargetEntry::new(
                "a",
                Expr::Var(Var {
                    varno: 1,
                    varattno: 1,
                    varlevelsup: 0,
                    vartype: SqlType::new(SqlTypeKind::Int4),
                }),
                SqlType::new(SqlTypeKind::Int4),
                1,
            )],
            where_qual: None,
            group_by: Vec::new(),
            accumulators: Vec::new(),
            window_clauses: Vec::new(),
            having_qual: None,
            sort_clause: Vec::new(),
            limit_count: None,
            limit_offset: 0,
            project_set: None,
            recursive_union: None,
            set_operation: None,
        }
    }

    fn base_rte() -> RangeTblEntry {
        RangeTblEntry {
            alias: None,
            desc: RelationDesc {
                columns: Vec::new(),
            },
            inh: false,
            kind: RangeTblEntryKind::Result,
        }
    }

    fn int4_var(varno: usize) -> Expr {
        Expr::Var(Var {
            varno,
            varattno: 1,
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Int4),
        })
    }

    fn eq_qual(left_varno: usize, right_varno: usize) -> Expr {
        Expr::Op(Box::new(OpExpr {
            opno: 0,
            opfuncid: 0,
            op: OpExprKind::Eq,
            opresulttype: SqlType::new(SqlTypeKind::Bool),
            args: vec![int4_var(left_varno), int4_var(right_varno)],
        }))
    }

    fn nested_left_join_query(upper_qual: Expr) -> Query {
        query_for_jointree(
            JoinTreeNode::JoinExpr {
                left: Box::new(JoinTreeNode::RangeTblRef(1)),
                right: Box::new(JoinTreeNode::JoinExpr {
                    left: Box::new(JoinTreeNode::RangeTblRef(2)),
                    right: Box::new(JoinTreeNode::RangeTblRef(3)),
                    kind: JoinType::Left,
                    rtindex: 4,
                    quals: eq_qual(2, 3),
                }),
                kind: JoinType::Left,
                rtindex: 5,
                quals: upper_qual,
            },
            vec![base_rte(), base_rte(), base_rte(), base_rte(), base_rte()],
        )
    }

    #[test]
    fn strict_nested_left_join_tracks_commute_sets() {
        let joins = build_special_join_info(&nested_left_join_query(eq_qual(1, 2)));
        assert_eq!(joins.len(), 2);

        let lower = joins.iter().find(|sj| sj.rtindex == 4).expect("lower join");
        let upper = joins.iter().find(|sj| sj.rtindex == 5).expect("upper join");

        assert_eq!(lower.ojrelid, Some(4));
        assert_eq!(upper.ojrelid, Some(5));
        assert_eq!(lower.commute_above_r, vec![5]);
        assert_eq!(upper.commute_below_r, vec![4]);
        assert!(lower.commute_above_l.is_empty());
        assert!(upper.commute_below_l.is_empty());
    }

    #[test]
    fn non_strict_nested_left_join_does_not_commute() {
        let joins = build_special_join_info(&nested_left_join_query(Expr::IsNotDistinctFrom(
            Box::new(int4_var(1)),
            Box::new(int4_var(2)),
        )));
        assert_eq!(joins.len(), 2);

        let lower = joins.iter().find(|sj| sj.rtindex == 4).expect("lower join");
        let upper = joins.iter().find(|sj| sj.rtindex == 5).expect("upper join");

        assert!(lower.commute_above_l.is_empty());
        assert!(lower.commute_above_r.is_empty());
        assert!(upper.commute_below_l.is_empty());
        assert!(upper.commute_below_r.is_empty());
    }

    #[test]
    fn full_join_is_recorded_as_hard_barrier() {
        let query = query_for_jointree(
            JoinTreeNode::JoinExpr {
                left: Box::new(JoinTreeNode::RangeTblRef(1)),
                right: Box::new(JoinTreeNode::RangeTblRef(2)),
                kind: JoinType::Full,
                rtindex: 3,
                quals: Expr::Const(crate::include::nodes::datum::Value::Bool(true)),
            },
            vec![base_rte(), base_rte(), base_rte()],
        );
        let joins = build_special_join_info(&query);
        assert_eq!(joins.len(), 1);
        assert_eq!(joins[0].ojrelid, Some(3));
        assert!(joins[0].commute_above_l.is_empty());
        assert!(joins[0].commute_above_r.is_empty());
    }

    #[test]
    fn outer_join_true_qual_backfills_empty_min_relsets() {
        let query = query_for_jointree(
            JoinTreeNode::JoinExpr {
                left: Box::new(JoinTreeNode::RangeTblRef(1)),
                right: Box::new(JoinTreeNode::RangeTblRef(2)),
                kind: JoinType::Left,
                rtindex: 3,
                quals: Expr::Const(crate::include::nodes::datum::Value::Bool(true)),
            },
            vec![base_rte(), base_rte(), base_rte()],
        );
        let joins = build_special_join_info(&query);
        assert_eq!(joins.len(), 1);
        assert_eq!(joins[0].min_lefthand, vec![1]);
        assert_eq!(joins[0].min_righthand, vec![2]);
    }
}
