use super::query::shift_expr_rtindexes;
use crate::include::nodes::primnodes::{
    BoolExprType, Expr, SELF_ITEM_POINTER_ATTR_NO, set_returning_call_exprs,
};

pub(crate) fn predicate_implies_index_predicate(
    filter: Option<&Expr>,
    index_predicate: Option<&Expr>,
) -> bool {
    let Some(index_predicate) = index_predicate else {
        return true;
    };
    let Some(filter) = filter else {
        return false;
    };
    let index_conjuncts = flatten_and_conjuncts(index_predicate);
    let filter_conjuncts = flatten_and_conjuncts(filter);
    let index_conjuncts = index_conjuncts
        .iter()
        .map(canonicalize_predicate_expr)
        .collect::<Vec<_>>();
    let filter_conjuncts = filter_conjuncts
        .iter()
        .map(canonicalize_predicate_expr)
        .collect::<Vec<_>>();
    index_conjuncts
        .iter()
        .all(|conjunct| filter_conjuncts.contains(conjunct))
}

pub(crate) fn flatten_and_conjuncts(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => bool_expr
            .args
            .iter()
            .flat_map(flatten_and_conjuncts)
            .collect(),
        other => vec![other.clone()],
    }
}

fn canonicalize_predicate_expr(expr: &Expr) -> Expr {
    const CANONICAL_RTINDEX: usize = 1_000_000;

    match single_local_varno(expr) {
        Some(varno) if varno != CANONICAL_RTINDEX => {
            shift_expr_rtindexes(expr.clone(), CANONICAL_RTINDEX - varno)
        }
        _ => expr.clone(),
    }
}

fn single_local_varno(expr: &Expr) -> Option<usize> {
    fn visit(expr: &Expr, found: &mut Option<usize>) -> bool {
        match expr {
            Expr::Var(var) => {
                if var.varlevelsup != 0 {
                    return true;
                }
                match found {
                    Some(existing) => *existing == var.varno,
                    None => {
                        *found = Some(var.varno);
                        true
                    }
                }
            }
            Expr::Param(_) | Expr::Const(_) => true,
            Expr::Aggref(aggref) => {
                aggref.args.iter().all(|arg| visit(arg, found))
                    && aggref
                        .aggfilter
                        .as_ref()
                        .is_none_or(|expr| visit(expr, found))
            }
            Expr::WindowFunc(window_func) => {
                window_func.args.iter().all(|arg| visit(arg, found))
                    && match &window_func.kind {
                        crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) => {
                            aggref
                                .aggfilter
                                .as_ref()
                                .is_none_or(|expr| visit(expr, found))
                        }
                        crate::include::nodes::primnodes::WindowFuncKind::Builtin(_) => true,
                    }
            }
            Expr::Op(op) => op.args.iter().all(|arg| visit(arg, found)),
            Expr::Bool(bool_expr) => bool_expr.args.iter().all(|arg| visit(arg, found)),
            Expr::Case(case_expr) => {
                case_expr
                    .arg
                    .as_deref()
                    .is_none_or(|expr| visit(expr, found))
                    && case_expr
                        .args
                        .iter()
                        .all(|arm| visit(&arm.expr, found) && visit(&arm.result, found))
                    && visit(&case_expr.defresult, found)
            }
            Expr::CaseTest(_) => true,
            Expr::Func(func) => func.args.iter().all(|arg| visit(arg, found)),
            Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
                .into_iter()
                .all(|expr| visit(expr, found)),
            Expr::SubLink(sublink) => sublink
                .testexpr
                .as_deref()
                .is_none_or(|expr| visit(expr, found)),
            Expr::SubPlan(subplan) => subplan
                .testexpr
                .as_deref()
                .is_none_or(|expr| visit(expr, found)),
            Expr::ScalarArrayOp(saop) => visit(&saop.left, found) && visit(&saop.right, found),
            Expr::Cast(inner, _)
            | Expr::Collate { expr: inner, .. }
            | Expr::IsNull(inner)
            | Expr::IsNotNull(inner) => visit(inner, found),
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
                visit(expr, found)
                    && visit(pattern, found)
                    && escape.as_deref().is_none_or(|expr| visit(expr, found))
            }
            Expr::IsDistinctFrom(left, right)
            | Expr::IsNotDistinctFrom(left, right)
            | Expr::Coalesce(left, right) => visit(left, found) && visit(right, found),
            Expr::ArrayLiteral { elements, .. } => elements.iter().all(|expr| visit(expr, found)),
            Expr::Row { fields, .. } => fields.iter().all(|(_, expr)| visit(expr, found)),
            Expr::FieldSelect { expr, .. } => visit(expr, found),
            Expr::ArraySubscript { array, subscripts } => {
                visit(array, found)
                    && subscripts.iter().all(|subscript| {
                        subscript
                            .lower
                            .as_ref()
                            .is_none_or(|expr| visit(expr, found))
                            && subscript
                                .upper
                                .as_ref()
                                .is_none_or(|expr| visit(expr, found))
                    })
            }
            Expr::Xml(xml) => xml.child_exprs().all(|expr| visit(expr, found)),
            Expr::Random
            | Expr::CurrentDate
            | Expr::CurrentCatalog
            | Expr::CurrentSchema
            | Expr::CurrentUser
            | Expr::SessionUser
            | Expr::CurrentRole
            | Expr::CurrentTime { .. }
            | Expr::CurrentTimestamp { .. }
            | Expr::LocalTime { .. }
            | Expr::LocalTimestamp { .. } => true,
        }
    }

    let mut found = None;
    visit(expr, &mut found).then_some(found).flatten()
}

pub(crate) fn expr_uses_ctid(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varattno == SELF_ITEM_POINTER_ATTR_NO,
        Expr::Param(_) | Expr::Const(_) => false,
        Expr::Aggref(aggref) => {
            aggref.args.iter().any(expr_uses_ctid)
                || aggref.aggfilter.as_ref().is_some_and(expr_uses_ctid)
        }
        Expr::WindowFunc(window_func) => {
            window_func.args.iter().any(expr_uses_ctid)
                || match &window_func.kind {
                    crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) => {
                        aggref.aggfilter.as_ref().is_some_and(expr_uses_ctid)
                    }
                    crate::include::nodes::primnodes::WindowFuncKind::Builtin(_) => false,
                }
        }
        Expr::Op(op) => op.args.iter().any(expr_uses_ctid),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_uses_ctid),
        Expr::Case(case_expr) => {
            case_expr.arg.as_deref().is_some_and(expr_uses_ctid)
                || case_expr
                    .args
                    .iter()
                    .any(|arm| expr_uses_ctid(&arm.expr) || expr_uses_ctid(&arm.result))
                || expr_uses_ctid(&case_expr.defresult)
        }
        Expr::CaseTest(_) => false,
        Expr::Func(func) => func.args.iter().any(expr_uses_ctid),
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .any(expr_uses_ctid),
        Expr::SubLink(sublink) => sublink.testexpr.as_deref().is_some_and(expr_uses_ctid),
        Expr::SubPlan(subplan) => subplan.testexpr.as_deref().is_some_and(expr_uses_ctid),
        Expr::ScalarArrayOp(saop) => expr_uses_ctid(&saop.left) || expr_uses_ctid(&saop.right),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => expr_uses_ctid(inner),
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
            expr_uses_ctid(expr)
                || expr_uses_ctid(pattern)
                || escape.as_deref().is_some_and(expr_uses_ctid)
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => expr_uses_ctid(left) || expr_uses_ctid(right),
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_uses_ctid),
        Expr::Row { fields, .. } => fields.iter().any(|(_, expr)| expr_uses_ctid(expr)),
        Expr::FieldSelect { expr, .. } => expr_uses_ctid(expr),
        Expr::ArraySubscript { array, subscripts } => {
            expr_uses_ctid(array)
                || subscripts.iter().any(|subscript| {
                    subscript.lower.as_ref().is_some_and(expr_uses_ctid)
                        || subscript.upper.as_ref().is_some_and(expr_uses_ctid)
                })
        }
        Expr::Xml(xml) => xml.child_exprs().any(expr_uses_ctid),
        Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}
