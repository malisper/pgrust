use crate::include::executor::execdesc::CommandType;
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{
    JoinTreeNode, Query, RangeTblEntry, RangeTblEntryKind, RangeTblEref,
};
use crate::include::nodes::primnodes::{
    Aggref, BoolExpr, BoolExprType, CaseExpr, CaseWhen, Expr, ExprArraySubscript, FuncExpr,
    JoinType, OpExpr, OpExprKind, OrderByEntry, RelationDesc, ScalarArrayOpExpr,
    SqlJsonQueryFunction, SqlJsonTableBehavior, SqlJsonTablePassingArg, SubLink, SubLinkType, Var,
    WindowFuncExpr, WindowFuncKind, XmlExpr, set_returning_call_exprs, user_attrno,
};

use super::{and_exprs, expr_relids, flatten_and_conjuncts, joininfo, relids_subset};
use crate::backend::parser::{SqlType, SqlTypeKind, SubqueryComparisonOp};

pub(super) fn pull_up_sublinks(mut query: Query) -> Query {
    hoist_inner_join_sublinks_to_where(&mut query);

    let Some(where_qual) = query.where_qual.take() else {
        return query;
    };

    let mut remaining_quals = Vec::new();
    for qual in flatten_and_conjuncts(&where_qual) {
        if pull_up_sublinks_qual_recurse(&mut query, qual.clone(), 0) {
            continue;
        }
        remaining_quals.push(qual);
    }
    query.where_qual = and_exprs(remaining_quals);
    query
}

fn hoist_inner_join_sublinks_to_where(query: &mut Query) {
    let Some(mut jointree) = query.jointree.take() else {
        return;
    };
    let mut hoisted_quals = Vec::new();
    hoist_inner_join_sublinks_recurse(&mut jointree, false, &mut hoisted_quals);
    query.jointree = Some(jointree);

    for qual in hoisted_quals {
        query.where_qual = match query.where_qual.take() {
            Some(where_qual) => Some(Expr::and(where_qual, qual)),
            None => Some(qual),
        };
    }
}

fn hoist_inner_join_sublinks_recurse(
    node: &mut JoinTreeNode,
    under_non_inner_join: bool,
    hoisted_quals: &mut Vec<Expr>,
) {
    let JoinTreeNode::JoinExpr {
        left,
        right,
        kind,
        quals,
        ..
    } = node
    else {
        return;
    };

    let is_inner_join = matches!(kind, JoinType::Inner | JoinType::Cross);
    let child_under_non_inner_join = under_non_inner_join || !is_inner_join;
    hoist_inner_join_sublinks_recurse(left, child_under_non_inner_join, hoisted_quals);
    hoist_inner_join_sublinks_recurse(right, child_under_non_inner_join, hoisted_quals);

    if under_non_inner_join || !is_inner_join {
        return;
    }

    let mut remaining_quals = Vec::new();
    for qual in flatten_and_conjuncts(quals) {
        if any_sublink(qual.clone()).is_some()
            || exists_sublink(qual.clone()).is_some()
            || not_exists_sublink(qual.clone()).is_some()
        {
            hoisted_quals.push(qual);
        } else {
            remaining_quals.push(qual);
        }
    }
    *quals = and_exprs(remaining_quals).unwrap_or_else(|| Expr::Const(Value::Bool(true)));
}

fn pull_up_sublinks_qual_recurse(query: &mut Query, qual: Expr, levels_to_parent: usize) -> bool {
    if let Some(sublink) = any_sublink(qual.clone()) {
        return convert_any_sublink_to_join(query, sublink, levels_to_parent);
    }
    if let Some(sublink) = exists_sublink(qual.clone()) {
        return convert_exists_sublink_to_join(query, sublink, false, levels_to_parent);
    }
    if let Some(sublink) = not_exists_sublink(qual) {
        return convert_exists_sublink_to_join(query, sublink, true, levels_to_parent);
    }
    false
}

fn exists_sublink(expr: Expr) -> Option<SubLink> {
    let Expr::SubLink(sublink) = expr else {
        return None;
    };
    if sublink.testexpr.is_none() && matches!(sublink.sublink_type, SubLinkType::ExistsSubLink) {
        Some(*sublink)
    } else {
        None
    }
}

fn any_sublink(expr: Expr) -> Option<SubLink> {
    let Expr::SubLink(sublink) = expr else {
        return None;
    };
    if matches!(sublink.sublink_type, SubLinkType::AnySubLink(_)) && sublink.testexpr.is_some() {
        Some(*sublink)
    } else {
        None
    }
}

fn not_exists_sublink(expr: Expr) -> Option<SubLink> {
    let Expr::Bool(bool_expr) = expr else {
        return None;
    };
    if !matches!(bool_expr.boolop, BoolExprType::Not) || bool_expr.args.len() != 1 {
        return None;
    }
    exists_sublink(bool_expr.args.into_iter().next().unwrap())
}

fn convert_any_sublink_to_join(
    query: &mut Query,
    sublink: SubLink,
    levels_to_parent: usize,
) -> bool {
    let SubLinkType::AnySubLink(op) = sublink.sublink_type else {
        return false;
    };
    let Some(testexpr) = sublink.testexpr else {
        return false;
    };
    let Some(op_kind) = subquery_comparison_op_expr_kind(op) else {
        return false;
    };
    let mut subquery = *sublink.subselect;
    if !simple_any_query(&subquery) {
        return false;
    }
    if levels_to_parent != 0 || expr_contains_outer_var(&testexpr) {
        return false;
    }
    if subquery.target_list.len() != 1 {
        return false;
    }

    let Some(subquery_target) = subquery
        .target_list
        .first()
        .map(|target| target.expr.clone())
    else {
        return false;
    };
    let mut working = query.clone();
    if let Some(where_qual) = subquery.where_qual.take() {
        let mut remaining = Vec::new();
        for qual in flatten_and_conjuncts(&where_qual) {
            if pull_up_sublinks_qual_recurse(&mut working, qual.clone(), levels_to_parent + 1) {
                continue;
            }
            remaining.push(qual);
        }
        subquery.where_qual = and_exprs(remaining);
    }

    if subquery
        .where_qual
        .as_ref()
        .is_some_and(expr_contains_sublink)
    {
        return false;
    }

    let offset = working.rtable.len();
    let Some(pulled_up_target) = adjust_expr_for_pullup(subquery_target, offset, levels_to_parent)
    else {
        return false;
    };
    let Some(right_tree) = append_pulled_up_subquery(&mut working, subquery, levels_to_parent)
    else {
        return false;
    };
    let comparison_qual = Expr::op(
        op_kind,
        SqlType::new(SqlTypeKind::Bool),
        vec![*testexpr, pulled_up_target],
    );
    let mut join_quals = working.where_qual.take().into_iter().collect::<Vec<_>>();
    join_quals.push(comparison_qual);
    let Some(join_quals) = and_exprs(join_quals) else {
        return false;
    };
    if !attach_pulled_up_join(&mut working, right_tree, JoinType::Semi, join_quals) {
        return false;
    }
    *query = working;
    true
}

fn subquery_comparison_op_expr_kind(op: SubqueryComparisonOp) -> Option<OpExprKind> {
    Some(match op {
        SubqueryComparisonOp::Eq => OpExprKind::Eq,
        SubqueryComparisonOp::NotEq => OpExprKind::NotEq,
        SubqueryComparisonOp::Lt => OpExprKind::Lt,
        SubqueryComparisonOp::LtEq => OpExprKind::LtEq,
        SubqueryComparisonOp::Gt => OpExprKind::Gt,
        SubqueryComparisonOp::GtEq => OpExprKind::GtEq,
        SubqueryComparisonOp::Match
        | SubqueryComparisonOp::Like
        | SubqueryComparisonOp::NotLike
        | SubqueryComparisonOp::ILike
        | SubqueryComparisonOp::NotILike
        | SubqueryComparisonOp::Similar
        | SubqueryComparisonOp::NotSimilar => return None,
    })
}

fn convert_exists_sublink_to_join(
    query: &mut Query,
    sublink: SubLink,
    under_not: bool,
    levels_to_parent: usize,
) -> bool {
    if !matches!(sublink.sublink_type, SubLinkType::ExistsSubLink) || sublink.testexpr.is_some() {
        return false;
    }
    let mut subquery = *sublink.subselect;
    if !simple_exists_query(&subquery) {
        return false;
    }

    let mut working = query.clone();
    if let Some(where_qual) = subquery.where_qual.take() {
        let mut remaining = Vec::new();
        for qual in flatten_and_conjuncts(&where_qual) {
            if pull_up_sublinks_qual_recurse(&mut working, qual.clone(), levels_to_parent + 1) {
                continue;
            }
            remaining.push(qual);
        }
        subquery.where_qual = and_exprs(remaining);
    }

    if subquery
        .where_qual
        .as_ref()
        .is_some_and(expr_contains_sublink)
    {
        return false;
    }

    let Some(right_tree) = append_pulled_up_subquery(&mut working, subquery, levels_to_parent)
    else {
        return false;
    };
    let join_quals = pulled_up_join_quals(&mut working, &right_tree)
        .unwrap_or_else(|| Expr::Const(Value::Bool(true)));
    let kind = if under_not {
        JoinType::Anti
    } else {
        JoinType::Semi
    };
    if !attach_pulled_up_join(&mut working, right_tree, kind, join_quals) {
        return false;
    }
    *query = working;
    true
}

fn simple_exists_query(query: &Query) -> bool {
    matches!(query.command_type, CommandType::Select)
        && query.group_by.is_empty()
        && query.accumulators.is_empty()
        && query.window_clauses.is_empty()
        && query.having_qual.is_none()
        && query.sort_clause.is_empty()
        && query.limit_count.is_none()
        && query.limit_offset.is_none()
        && query.locking_clause.is_none()
        && query.row_marks.is_empty()
        && !query.has_target_srfs
        && query.recursive_union.is_none()
        && query.set_operation.is_none()
        && query.jointree.is_some()
        && query.rtable.iter().all(supported_pulled_up_rte)
}

fn simple_any_query(query: &Query) -> bool {
    simple_exists_query(query)
        && query.target_list.len() == 1
        && !query.target_list[0].resjunk
        && !expr_contains_sublink(&query.target_list[0].expr)
}

fn supported_pulled_up_rte(rte: &RangeTblEntry) -> bool {
    matches!(
        rte.kind,
        RangeTblEntryKind::Relation { .. } | RangeTblEntryKind::Join { .. }
    )
}

fn append_pulled_up_subquery(
    query: &mut Query,
    mut subquery: Query,
    levels_to_parent: usize,
) -> Option<JoinTreeNode> {
    let offset = query.rtable.len();
    let jointree = adjust_jointree_for_pullup(subquery.jointree.take()?, offset, levels_to_parent)?;
    let rtable = subquery
        .rtable
        .into_iter()
        .map(|rte| adjust_rte_for_pullup(rte, offset, levels_to_parent))
        .collect::<Option<Vec<_>>>()?;
    let where_qual = adjust_optional_expr(subquery.where_qual, offset, levels_to_parent)?;
    query.depends_on_row_security |= subquery.depends_on_row_security;
    query.rtable.extend(rtable);
    query.where_qual = match (query.where_qual.take(), where_qual) {
        (Some(left), Some(right)) => Some(Expr::and(left, right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    };
    Some(jointree)
}

fn pulled_up_join_quals(query: &mut Query, _right_tree: &JoinTreeNode) -> Option<Expr> {
    query.where_qual.take()
}

fn attach_pulled_up_join(
    query: &mut Query,
    right_tree: JoinTreeNode,
    kind: JoinType,
    join_quals: Expr,
) -> bool {
    let join_quals = joininfo::flatten_join_alias_vars_query(query, join_quals);
    if expr_contains_outer_var(&join_quals) {
        return false;
    }
    let Some(left_tree) = query.jointree.take() else {
        return false;
    };
    if matches!(kind, JoinType::Anti) {
        let right_relids = jointree_relids(&right_tree);
        let mut left_required = expr_relids(&join_quals)
            .into_iter()
            .filter(|relid| !right_relids.contains(relid))
            .collect::<Vec<_>>();
        left_required.sort_unstable();
        left_required.dedup();
        if !left_required.is_empty()
            && let Some(jointree) = insert_pulled_up_join_at_relids(
                query,
                left_tree.clone(),
                &left_required,
                right_tree.clone(),
                kind,
                join_quals.clone(),
            )
        {
            query.jointree = Some(jointree);
            return true;
        }
    }
    let Some(jointree) = make_pulled_up_join(query, left_tree, right_tree, kind, join_quals) else {
        return false;
    };
    query.jointree = Some(jointree);
    true
}

fn insert_pulled_up_join_at_relids(
    query: &mut Query,
    node: JoinTreeNode,
    required_relids: &[usize],
    right_tree: JoinTreeNode,
    kind: JoinType,
    join_quals: Expr,
) -> Option<JoinTreeNode> {
    let node_relids = jointree_relids(&node);
    if node_relids == required_relids {
        return make_pulled_up_join(query, node, right_tree, kind, join_quals);
    }
    match node {
        JoinTreeNode::RangeTblRef(_) => None,
        JoinTreeNode::JoinExpr {
            left,
            right,
            kind: existing_kind,
            quals,
            rtindex,
        } => {
            let left_relids = jointree_relids(&left);
            let right_relids = jointree_relids(&right);
            if relids_subset(required_relids, &left_relids) {
                Some(JoinTreeNode::JoinExpr {
                    left: Box::new(insert_pulled_up_join_at_relids(
                        query,
                        *left,
                        required_relids,
                        right_tree,
                        kind,
                        join_quals,
                    )?),
                    right,
                    kind: existing_kind,
                    quals,
                    rtindex,
                })
            } else if relids_subset(required_relids, &right_relids) {
                Some(JoinTreeNode::JoinExpr {
                    left,
                    right: Box::new(insert_pulled_up_join_at_relids(
                        query,
                        *right,
                        required_relids,
                        right_tree,
                        kind,
                        join_quals,
                    )?),
                    kind: existing_kind,
                    quals,
                    rtindex,
                })
            } else {
                None
            }
        }
    }
}

fn make_pulled_up_join(
    query: &mut Query,
    left_tree: JoinTreeNode,
    right_tree: JoinTreeNode,
    kind: JoinType,
    join_quals: Expr,
) -> Option<JoinTreeNode> {
    let left_desc = jointree_desc(query, &left_tree)?;
    let left_alias_vars = jointree_output_exprs(query, &left_tree)?;
    let join_rtindex = query.rtable.len() + 1;
    let joinleftcols = (1..=left_desc.columns.len()).collect::<Vec<_>>();
    query.rtable.push(RangeTblEntry {
        alias: None,
        alias_preserves_source_names: false,
        eref: RangeTblEref {
            aliasname: "join".into(),
            colnames: left_desc
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
        },
        desc: left_desc.clone(),
        inh: false,
        security_quals: Vec::new(),
        permission: None,
        kind: RangeTblEntryKind::Join {
            jointype: kind,
            joinmergedcols: 0,
            joinaliasvars: left_alias_vars,
            joinleftcols,
            joinrightcols: Vec::new(),
        },
    });
    Some(JoinTreeNode::JoinExpr {
        left: Box::new(left_tree),
        right: Box::new(right_tree),
        kind,
        quals: join_quals,
        rtindex: join_rtindex,
    })
}

fn jointree_relids(node: &JoinTreeNode) -> Vec<usize> {
    match node {
        JoinTreeNode::RangeTblRef(rtindex) => vec![*rtindex],
        JoinTreeNode::JoinExpr { left, right, .. } => {
            let mut relids = jointree_relids(left);
            relids.extend(jointree_relids(right));
            relids.sort_unstable();
            relids.dedup();
            relids
        }
    }
}

fn adjust_jointree_for_pullup(
    node: JoinTreeNode,
    offset: usize,
    levels_to_parent: usize,
) -> Option<JoinTreeNode> {
    match node {
        JoinTreeNode::RangeTblRef(rtindex) => Some(JoinTreeNode::RangeTblRef(rtindex + offset)),
        JoinTreeNode::JoinExpr {
            left,
            right,
            kind,
            quals,
            rtindex,
        } => Some(JoinTreeNode::JoinExpr {
            left: Box::new(adjust_jointree_for_pullup(*left, offset, levels_to_parent)?),
            right: Box::new(adjust_jointree_for_pullup(
                *right,
                offset,
                levels_to_parent,
            )?),
            kind,
            quals: adjust_expr_for_pullup(quals, offset, levels_to_parent)?,
            rtindex: rtindex + offset,
        }),
    }
}

fn adjust_rte_for_pullup(
    rte: RangeTblEntry,
    offset: usize,
    levels_to_parent: usize,
) -> Option<RangeTblEntry> {
    let security_quals = rte
        .security_quals
        .into_iter()
        .map(|expr| adjust_expr_for_pullup(expr, offset, levels_to_parent))
        .collect::<Option<Vec<_>>>()?;
    let kind = match rte.kind {
        RangeTblEntryKind::Join {
            jointype,
            joinmergedcols,
            joinaliasvars,
            joinleftcols,
            joinrightcols,
        } => RangeTblEntryKind::Join {
            jointype,
            joinmergedcols,
            joinaliasvars: joinaliasvars
                .into_iter()
                .map(|expr| adjust_expr_for_pullup(expr, offset, levels_to_parent))
                .collect::<Option<Vec<_>>>()?,
            joinleftcols,
            joinrightcols,
        },
        kind @ RangeTblEntryKind::Relation { .. } => kind,
        _ => return None,
    };
    Some(RangeTblEntry {
        security_quals,
        kind,
        ..rte
    })
}

fn adjust_expr_for_pullup(expr: Expr, offset: usize, levels_to_parent: usize) -> Option<Expr> {
    Some(match expr {
        Expr::Var(mut var) => {
            if var.varlevelsup == 0 {
                var.varno += offset;
            } else if var.varlevelsup <= levels_to_parent {
                return None;
            } else if var.varlevelsup == levels_to_parent + 1 {
                var.varlevelsup = 0;
            } else {
                var.varlevelsup -= levels_to_parent + 1;
            }
            Expr::Var(var)
        }
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(adjust_aggref_for_pullup(
            *aggref,
            offset,
            levels_to_parent,
        )?)),
        Expr::WindowFunc(window_func) => Expr::WindowFunc(Box::new(adjust_window_func_for_pullup(
            *window_func,
            offset,
            levels_to_parent,
        )?)),
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: adjust_exprs_for_pullup(op.args, offset, levels_to_parent)?,
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: adjust_exprs_for_pullup(bool_expr.args, offset, levels_to_parent)?,
            ..*bool_expr
        })),
        Expr::Case(case_expr) => Expr::Case(Box::new(adjust_case_for_pullup(
            *case_expr,
            offset,
            levels_to_parent,
        )?)),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: adjust_exprs_for_pullup(func.args, offset, levels_to_parent)?,
            ..*func
        })),
        Expr::SqlJsonQueryFunction(func) => {
            Expr::SqlJsonQueryFunction(Box::new(SqlJsonQueryFunction {
                context: adjust_expr_for_pullup(func.context, offset, levels_to_parent)?,
                path: adjust_expr_for_pullup(func.path, offset, levels_to_parent)?,
                passing: func
                    .passing
                    .into_iter()
                    .map(|arg| {
                        Some(SqlJsonTablePassingArg {
                            name: arg.name,
                            expr: adjust_expr_for_pullup(arg.expr, offset, levels_to_parent)?,
                        })
                    })
                    .collect::<Option<Vec<_>>>()?,
                on_empty: adjust_sql_json_behavior_for_pullup(
                    func.on_empty,
                    offset,
                    levels_to_parent,
                )?,
                on_error: adjust_sql_json_behavior_for_pullup(
                    func.on_error,
                    offset,
                    levels_to_parent,
                )?,
                ..*func
            }))
        }
        Expr::SetReturning(_) => return None,
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(SubLink {
            testexpr: adjust_optional_box_expr(sublink.testexpr, offset, levels_to_parent)?,
            subselect: sublink.subselect,
            sublink_type: sublink.sublink_type,
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(adjust_expr_for_pullup(
                *saop.left,
                offset,
                levels_to_parent,
            )?),
            right: Box::new(adjust_expr_for_pullup(
                *saop.right,
                offset,
                levels_to_parent,
            )?),
            ..*saop
        })),
        Expr::Xml(xml) => Expr::Xml(Box::new(XmlExpr {
            named_args: adjust_exprs_for_pullup(xml.named_args, offset, levels_to_parent)?,
            args: adjust_exprs_for_pullup(xml.args, offset, levels_to_parent)?,
            ..*xml
        })),
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(adjust_expr_for_pullup(*inner, offset, levels_to_parent)?),
            ty,
        ),
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(adjust_expr_for_pullup(*expr, offset, levels_to_parent)?),
            collation_oid,
        },
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
        } => Expr::Like {
            expr: Box::new(adjust_expr_for_pullup(*expr, offset, levels_to_parent)?),
            pattern: Box::new(adjust_expr_for_pullup(*pattern, offset, levels_to_parent)?),
            escape: adjust_optional_box_expr(escape, offset, levels_to_parent)?,
            case_insensitive,
            negated,
            collation_oid,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            collation_oid,
        } => Expr::Similar {
            expr: Box::new(adjust_expr_for_pullup(*expr, offset, levels_to_parent)?),
            pattern: Box::new(adjust_expr_for_pullup(*pattern, offset, levels_to_parent)?),
            escape: adjust_optional_box_expr(escape, offset, levels_to_parent)?,
            negated,
            collation_oid,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(adjust_expr_for_pullup(
            *inner,
            offset,
            levels_to_parent,
        )?)),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(adjust_expr_for_pullup(
            *inner,
            offset,
            levels_to_parent,
        )?)),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(adjust_expr_for_pullup(*left, offset, levels_to_parent)?),
            Box::new(adjust_expr_for_pullup(*right, offset, levels_to_parent)?),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(adjust_expr_for_pullup(*left, offset, levels_to_parent)?),
            Box::new(adjust_expr_for_pullup(*right, offset, levels_to_parent)?),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: adjust_exprs_for_pullup(elements, offset, levels_to_parent)?,
            array_type,
        },
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| {
                    adjust_expr_for_pullup(expr, offset, levels_to_parent).map(|expr| (name, expr))
                })
                .collect::<Option<Vec<_>>>()?,
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(adjust_expr_for_pullup(*expr, offset, levels_to_parent)?),
            field,
            field_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(adjust_expr_for_pullup(*left, offset, levels_to_parent)?),
            Box::new(adjust_expr_for_pullup(*right, offset, levels_to_parent)?),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(adjust_expr_for_pullup(*array, offset, levels_to_parent)?),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| {
                    Some(ExprArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: adjust_optional_expr(subscript.lower, offset, levels_to_parent)?,
                        upper: adjust_optional_expr(subscript.upper, offset, levels_to_parent)?,
                    })
                })
                .collect::<Option<Vec<_>>>()?,
        },
        Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::SubPlan(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => expr,
    })
}

fn adjust_sql_json_behavior_for_pullup(
    behavior: SqlJsonTableBehavior,
    offset: usize,
    levels_to_parent: usize,
) -> Option<SqlJsonTableBehavior> {
    Some(match behavior {
        SqlJsonTableBehavior::Default(expr) => {
            SqlJsonTableBehavior::Default(adjust_expr_for_pullup(expr, offset, levels_to_parent)?)
        }
        other => other,
    })
}

fn adjust_exprs_for_pullup(
    exprs: Vec<Expr>,
    offset: usize,
    levels_to_parent: usize,
) -> Option<Vec<Expr>> {
    exprs
        .into_iter()
        .map(|expr| adjust_expr_for_pullup(expr, offset, levels_to_parent))
        .collect()
}

fn adjust_optional_expr(
    expr: Option<Expr>,
    offset: usize,
    levels_to_parent: usize,
) -> Option<Option<Expr>> {
    expr.map_or(Some(None), |expr| {
        adjust_expr_for_pullup(expr, offset, levels_to_parent).map(Some)
    })
}

fn adjust_optional_box_expr(
    expr: Option<Box<Expr>>,
    offset: usize,
    levels_to_parent: usize,
) -> Option<Option<Box<Expr>>> {
    expr.map_or(Some(None), |expr| {
        adjust_expr_for_pullup(*expr, offset, levels_to_parent)
            .map(Box::new)
            .map(Some)
    })
}

fn adjust_order_by_for_pullup(
    items: Vec<OrderByEntry>,
    offset: usize,
    levels_to_parent: usize,
) -> Option<Vec<OrderByEntry>> {
    items
        .into_iter()
        .map(|item| {
            Some(OrderByEntry {
                expr: adjust_expr_for_pullup(item.expr, offset, levels_to_parent)?,
                ..item
            })
        })
        .collect()
}

fn adjust_aggref_for_pullup(
    aggref: Aggref,
    offset: usize,
    levels_to_parent: usize,
) -> Option<Aggref> {
    Some(Aggref {
        args: adjust_exprs_for_pullup(aggref.args, offset, levels_to_parent)?,
        aggorder: adjust_order_by_for_pullup(aggref.aggorder, offset, levels_to_parent)?,
        aggfilter: adjust_optional_expr(aggref.aggfilter, offset, levels_to_parent)?,
        ..aggref
    })
}

fn adjust_window_func_for_pullup(
    window_func: WindowFuncExpr,
    offset: usize,
    levels_to_parent: usize,
) -> Option<WindowFuncExpr> {
    Some(WindowFuncExpr {
        kind: match window_func.kind {
            WindowFuncKind::Aggregate(aggref) => WindowFuncKind::Aggregate(
                adjust_aggref_for_pullup(aggref, offset, levels_to_parent)?,
            ),
            kind @ WindowFuncKind::Builtin(_) => kind,
        },
        args: adjust_exprs_for_pullup(window_func.args, offset, levels_to_parent)?,
        ..window_func
    })
}

fn adjust_case_for_pullup(
    case_expr: CaseExpr,
    offset: usize,
    levels_to_parent: usize,
) -> Option<CaseExpr> {
    Some(CaseExpr {
        arg: adjust_optional_box_expr(case_expr.arg, offset, levels_to_parent)?,
        args: case_expr
            .args
            .into_iter()
            .map(|arm| {
                Some(CaseWhen {
                    expr: adjust_expr_for_pullup(arm.expr, offset, levels_to_parent)?,
                    result: adjust_expr_for_pullup(arm.result, offset, levels_to_parent)?,
                })
            })
            .collect::<Option<Vec<_>>>()?,
        defresult: Box::new(adjust_expr_for_pullup(
            *case_expr.defresult,
            offset,
            levels_to_parent,
        )?),
        ..case_expr
    })
}

fn expr_contains_sublink(expr: &Expr) -> bool {
    match expr {
        Expr::SubLink(_) => true,
        Expr::Aggref(aggref) => {
            aggref.args.iter().any(expr_contains_sublink)
                || aggref
                    .aggorder
                    .iter()
                    .any(|item| expr_contains_sublink(&item.expr))
                || aggref.aggfilter.as_ref().is_some_and(expr_contains_sublink)
        }
        Expr::WindowFunc(window_func) => {
            window_func.args.iter().any(expr_contains_sublink)
                || match &window_func.kind {
                    WindowFuncKind::Aggregate(aggref) => {
                        expr_contains_sublink(&Expr::Aggref(Box::new(aggref.clone())))
                    }
                    WindowFuncKind::Builtin(_) => false,
                }
        }
        Expr::Op(op) => op.args.iter().any(expr_contains_sublink),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_contains_sublink),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_some_and(|arg| expr_contains_sublink(arg))
                || case_expr.args.iter().any(|arm| {
                    expr_contains_sublink(&arm.expr) || expr_contains_sublink(&arm.result)
                })
                || expr_contains_sublink(&case_expr.defresult)
        }
        Expr::Func(func) => func.args.iter().any(expr_contains_sublink),
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .any(expr_contains_sublink),
        Expr::ScalarArrayOp(saop) => {
            expr_contains_sublink(&saop.left) || expr_contains_sublink(&saop.right)
        }
        Expr::Xml(xml) => xml.child_exprs().any(expr_contains_sublink),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => expr_contains_sublink(inner),
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
            expr_contains_sublink(expr)
                || expr_contains_sublink(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_contains_sublink(expr))
        }
        Expr::SqlJsonQueryFunction(func) => {
            func.child_exprs().into_iter().any(expr_contains_sublink)
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_contains_sublink(left) || expr_contains_sublink(right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_contains_sublink),
        Expr::Row { fields, .. } => fields.iter().any(|(_, expr)| expr_contains_sublink(expr)),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_sublink(array)
                || subscripts.iter().any(|subscript| {
                    subscript.lower.as_ref().is_some_and(expr_contains_sublink)
                        || subscript.upper.as_ref().is_some_and(expr_contains_sublink)
                })
        }
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::SubPlan(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
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

fn expr_contains_outer_var(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varlevelsup > 0,
        Expr::Aggref(aggref) => {
            aggref.args.iter().any(expr_contains_outer_var)
                || aggref
                    .aggorder
                    .iter()
                    .any(|item| expr_contains_outer_var(&item.expr))
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(expr_contains_outer_var)
        }
        Expr::WindowFunc(window_func) => {
            window_func.args.iter().any(expr_contains_outer_var)
                || match &window_func.kind {
                    WindowFuncKind::Aggregate(aggref) => {
                        expr_contains_outer_var(&Expr::Aggref(Box::new(aggref.clone())))
                    }
                    WindowFuncKind::Builtin(_) => false,
                }
        }
        Expr::Op(op) => op.args.iter().any(expr_contains_outer_var),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_contains_outer_var),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_some_and(|arg| expr_contains_outer_var(arg))
                || case_expr.args.iter().any(|arm| {
                    expr_contains_outer_var(&arm.expr) || expr_contains_outer_var(&arm.result)
                })
                || expr_contains_outer_var(&case_expr.defresult)
        }
        Expr::Func(func) => func.args.iter().any(expr_contains_outer_var),
        Expr::SqlJsonQueryFunction(func) => {
            func.child_exprs().into_iter().any(expr_contains_outer_var)
        }
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .any(expr_contains_outer_var),
        Expr::SubLink(sublink) => sublink
            .testexpr
            .as_ref()
            .is_some_and(|expr| expr_contains_outer_var(expr)),
        Expr::SubPlan(subplan) => {
            subplan
                .testexpr
                .as_ref()
                .is_some_and(|expr| expr_contains_outer_var(expr))
                || subplan.args.iter().any(expr_contains_outer_var)
        }
        Expr::ScalarArrayOp(saop) => {
            expr_contains_outer_var(&saop.left) || expr_contains_outer_var(&saop.right)
        }
        Expr::Xml(xml) => xml.child_exprs().any(expr_contains_outer_var),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => expr_contains_outer_var(inner),
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
            expr_contains_outer_var(expr)
                || expr_contains_outer_var(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_contains_outer_var(expr))
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_contains_outer_var(left) || expr_contains_outer_var(right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_contains_outer_var),
        Expr::Row { fields, .. } => fields.iter().any(|(_, expr)| expr_contains_outer_var(expr)),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_outer_var(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(expr_contains_outer_var)
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(expr_contains_outer_var)
                })
        }
        Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
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

fn jointree_desc(query: &Query, node: &JoinTreeNode) -> Option<RelationDesc> {
    let rtindex = match node {
        JoinTreeNode::RangeTblRef(rtindex) => *rtindex,
        JoinTreeNode::JoinExpr { rtindex, .. } => *rtindex,
    };
    query
        .rtable
        .get(rtindex.checked_sub(1)?)
        .map(|rte| rte.desc.clone())
}

fn jointree_output_exprs(query: &Query, node: &JoinTreeNode) -> Option<Vec<Expr>> {
    let rtindex = match node {
        JoinTreeNode::RangeTblRef(rtindex) => *rtindex,
        JoinTreeNode::JoinExpr { rtindex, .. } => *rtindex,
    };
    let rte = query.rtable.get(rtindex.checked_sub(1)?)?;
    Some(
        rte.desc
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Expr::Var(Var {
                    varno: rtindex,
                    varattno: user_attrno(index),
                    varlevelsup: 0,
                    vartype: column.sql_type,
                })
            })
            .collect(),
    )
}
