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
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}
