use super::{is_text_like_type, query::shift_expr_rtindexes};
use crate::runtime::{cast_value, compare_order_values};
use pgrust_nodes::datum::Value;
use pgrust_nodes::primnodes::{
    BoolExprType, Expr, OpExprKind, SELF_ITEM_POINTER_ATTR_NO, expr_sql_type_hint,
    set_returning_call_exprs,
};

pub fn predicate_implies_index_predicate(
    filter: Option<&Expr>,
    index_predicate: Option<&Expr>,
) -> bool {
    let Some(index_predicate) = index_predicate else {
        return true;
    };
    let Some(filter) = filter else {
        return false;
    };
    predicate_expr_implies(
        &canonicalize_predicate_expr(filter),
        &canonicalize_predicate_expr(index_predicate),
    )
}

pub fn flatten_and_conjuncts(expr: &Expr) -> Vec<Expr> {
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

fn predicate_expr_implies(filter: &Expr, predicate: &Expr) -> bool {
    if filter == predicate || simple_comparison_implies(filter, predicate) {
        return true;
    }

    match predicate {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => bool_expr
            .args
            .iter()
            .all(|part| predicate_expr_implies(filter, part)),
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::Or => bool_expr
            .args
            .iter()
            .any(|part| predicate_expr_implies(filter, part)),
        _ => match filter {
            Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => bool_expr
                .args
                .iter()
                .any(|part| predicate_expr_implies(part, predicate)),
            Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::Or => bool_expr
                .args
                .iter()
                .all(|part| predicate_expr_implies(part, predicate)),
            _ => false,
        },
    }
}

#[derive(Debug, Clone)]
struct SimpleComparison {
    key: Expr,
    op: OpExprKind,
    value: Value,
}

fn simple_comparison_implies(filter: &Expr, predicate: &Expr) -> bool {
    let Some(filter) = extract_simple_comparison(filter) else {
        return false;
    };
    let Some(predicate) = extract_simple_comparison(predicate) else {
        return false;
    };
    if !predicate_keys_match(&filter.key, &predicate.key) {
        return false;
    }
    let Some(ordering) = compare_order_values(&filter.value, &predicate.value, None, None, false)
        .ok()
        .or_else(|| compare_predicate_integer_values(&filter.value, &predicate.value))
    else {
        return false;
    };
    match filter.op {
        OpExprKind::Eq => match predicate.op {
            OpExprKind::Eq => ordering.is_eq(),
            OpExprKind::Lt => ordering.is_lt(),
            OpExprKind::LtEq => !ordering.is_gt(),
            OpExprKind::Gt => ordering.is_gt(),
            OpExprKind::GtEq => !ordering.is_lt(),
            _ => false,
        },
        OpExprKind::Lt => match predicate.op {
            OpExprKind::Lt => !ordering.is_gt(),
            OpExprKind::LtEq => !ordering.is_gt(),
            _ => false,
        },
        OpExprKind::LtEq => match predicate.op {
            OpExprKind::Lt => ordering.is_lt(),
            OpExprKind::LtEq => !ordering.is_gt(),
            _ => false,
        },
        OpExprKind::Gt => match predicate.op {
            OpExprKind::Gt => !ordering.is_lt(),
            OpExprKind::GtEq => !ordering.is_lt(),
            _ => false,
        },
        OpExprKind::GtEq => match predicate.op {
            OpExprKind::Gt => ordering.is_gt(),
            OpExprKind::GtEq => !ordering.is_lt(),
            _ => false,
        },
        _ => false,
    }
}

fn predicate_keys_match(left: &Expr, right: &Expr) -> bool {
    left == right || normalize_predicate_key(left) == normalize_predicate_key(right)
}

fn normalize_predicate_key(expr: &Expr) -> Expr {
    match expr {
        Expr::Const(Value::Int16(value)) => Expr::Const(Value::Int64(i64::from(*value))),
        Expr::Const(Value::Int32(value)) => Expr::Const(Value::Int64(i64::from(*value))),
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(normalize_predicate_key(left)),
            Box::new(normalize_predicate_key(right)),
        ),
        Expr::Cast(inner, ty) => match inner.as_ref() {
            Expr::Const(value) => cast_value(value.clone(), *ty)
                .map(Expr::Const)
                .unwrap_or_else(|_| Expr::Cast(Box::new(normalize_predicate_key(inner)), *ty)),
            inner if expr_sql_type_hint(inner).is_some_and(|source| source == *ty) => {
                normalize_predicate_key(inner)
            }
            _ => Expr::Cast(Box::new(normalize_predicate_key(inner)), *ty),
        },
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(normalize_predicate_key(expr)),
            collation_oid: *collation_oid,
        },
        Expr::Func(func) => {
            let mut func = (**func).clone();
            func.args = func.args.iter().map(normalize_predicate_key).collect();
            Expr::Func(Box::new(func))
        }
        Expr::Op(op) => {
            let mut op = (**op).clone();
            op.args = op.args.iter().map(normalize_predicate_key).collect();
            Expr::Op(Box::new(op))
        }
        other => other.clone(),
    }
}

fn compare_predicate_integer_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    Some(predicate_integer_value(left)?.cmp(&predicate_integer_value(right)?))
}

fn predicate_integer_value(value: &Value) -> Option<i128> {
    match value {
        Value::Int16(value) => Some(i128::from(*value)),
        Value::Int32(value) => Some(i128::from(*value)),
        Value::Int64(value) => Some(i128::from(*value)),
        _ => None,
    }
}

fn extract_simple_comparison(expr: &Expr) -> Option<SimpleComparison> {
    let Expr::Op(op) = expr else {
        return None;
    };
    let op_kind = match op.op {
        OpExprKind::Eq | OpExprKind::Lt | OpExprKind::LtEq | OpExprKind::Gt | OpExprKind::GtEq => {
            op.op
        }
        _ => return None,
    };
    let [left, right] = op.args.as_slice() else {
        return None;
    };
    if let Some(value) = predicate_const_value(right) {
        return predicate_key_expr(left).map(|key| SimpleComparison {
            key,
            op: op_kind,
            value,
        });
    }
    if let Some(value) = predicate_const_value(left) {
        let op = commute_comparison_op(op_kind)?;
        return predicate_key_expr(right).map(|key| SimpleComparison { key, op, value });
    }
    None
}

fn commute_comparison_op(op: OpExprKind) -> Option<OpExprKind> {
    Some(match op {
        OpExprKind::Eq => OpExprKind::Eq,
        OpExprKind::Lt => OpExprKind::Gt,
        OpExprKind::LtEq => OpExprKind::GtEq,
        OpExprKind::Gt => OpExprKind::Lt,
        OpExprKind::GtEq => OpExprKind::LtEq,
        _ => return None,
    })
}

fn predicate_key_expr(expr: &Expr) -> Option<Expr> {
    let stripped = strip_text_like_casts(expr);
    if matches!(stripped, Expr::Const(_)) {
        return None;
    }
    Some(stripped.clone())
}

fn predicate_const_value(expr: &Expr) -> Option<Value> {
    match strip_text_like_casts(expr) {
        Expr::Const(value) => Some(value.clone()),
        Expr::Cast(inner, target) => {
            predicate_const_value(inner).and_then(|value| cast_value(value, *target).ok())
        }
        _ => None,
    }
}

fn strip_text_like_casts(expr: &Expr) -> &Expr {
    match expr {
        Expr::Cast(inner, target)
            if expr_sql_type_hint(inner)
                .is_some_and(|source| is_text_like_type(source) && is_text_like_type(*target)) =>
        {
            strip_text_like_casts(inner)
        }
        Expr::Collate { expr, .. } => strip_text_like_casts(expr),
        _ => expr,
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
            Expr::GroupingKey(grouping_key) => visit(&grouping_key.expr, found),
            Expr::GroupingFunc(grouping_func) => {
                grouping_func.args.iter().all(|arg| visit(arg, found))
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
                        pgrust_nodes::primnodes::WindowFuncKind::Aggregate(aggref) => aggref
                            .aggfilter
                            .as_ref()
                            .is_none_or(|expr| visit(expr, found)),
                        pgrust_nodes::primnodes::WindowFuncKind::Builtin(_) => true,
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
            Expr::SqlJsonQueryFunction(func) => func
                .child_exprs()
                .into_iter()
                .all(|expr| visit(expr, found)),
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
            | Expr::User
            | Expr::SystemUser
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

pub fn expr_uses_ctid(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varattno == SELF_ITEM_POINTER_ATTR_NO,
        Expr::GroupingKey(grouping_key) => expr_uses_ctid(&grouping_key.expr),
        Expr::GroupingFunc(grouping_func) => grouping_func.args.iter().any(expr_uses_ctid),
        Expr::Param(_) | Expr::Const(_) => false,
        Expr::Aggref(aggref) => {
            aggref.args.iter().any(expr_uses_ctid)
                || aggref.aggfilter.as_ref().is_some_and(expr_uses_ctid)
        }
        Expr::WindowFunc(window_func) => {
            window_func.args.iter().any(expr_uses_ctid)
                || match &window_func.kind {
                    pgrust_nodes::primnodes::WindowFuncKind::Aggregate(aggref) => {
                        aggref.aggfilter.as_ref().is_some_and(expr_uses_ctid)
                    }
                    pgrust_nodes::primnodes::WindowFuncKind::Builtin(_) => false,
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
        Expr::SqlJsonQueryFunction(func) => func.child_exprs().into_iter().any(expr_uses_ctid),
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
        | Expr::User
        | Expr::SystemUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
    use pgrust_nodes::primnodes::{OpExpr, Var, user_attrno};

    fn var() -> Expr {
        Expr::Var(Var {
            varno: 1,
            varattno: user_attrno(0),
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Name),
            collation_oid: None,
        })
    }

    fn cmp(op: OpExprKind, value: &str) -> Expr {
        Expr::Op(Box::new(OpExpr {
            opno: 0,
            opfuncid: 0,
            op,
            opresulttype: SqlType::new(SqlTypeKind::Bool),
            args: vec![var(), Expr::Const(Value::Text(value.into()))],
            collation_oid: None,
        }))
    }

    #[test]
    fn equality_filter_implies_text_range_partial_predicate() {
        assert!(predicate_implies_index_predicate(
            Some(&cmp(OpExprKind::Eq, "ATAAAA")),
            Some(&cmp(OpExprKind::Lt, "B")),
        ));
        assert!(!predicate_implies_index_predicate(
            Some(&cmp(OpExprKind::Eq, "C")),
            Some(&cmp(OpExprKind::Lt, "B")),
        ));
    }

    #[test]
    fn equality_filter_implies_matching_or_disjunct() {
        let predicate = Expr::or(cmp(OpExprKind::Lt, "B"), cmp(OpExprKind::Gt, "Y"));
        assert!(predicate_implies_index_predicate(
            Some(&cmp(OpExprKind::Eq, "A")),
            Some(&predicate),
        ));
    }

    fn int_var() -> Expr {
        Expr::Var(Var {
            varno: 1,
            varattno: user_attrno(0),
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Int4),
            collation_oid: None,
        })
    }

    fn int8_var() -> Expr {
        Expr::Var(Var {
            varno: 1,
            varattno: user_attrno(0),
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Int8),
            collation_oid: None,
        })
    }

    fn int_cmp(op: OpExprKind, value: i32) -> Expr {
        Expr::Op(Box::new(OpExpr {
            opno: 0,
            opfuncid: 0,
            op,
            opresulttype: SqlType::new(SqlTypeKind::Bool),
            args: vec![int_var(), Expr::Const(Value::Int32(value))],
            collation_oid: None,
        }))
    }

    fn coalesce_int_cmp(op: OpExprKind, value: i32, fallback: Value) -> Expr {
        Expr::Op(Box::new(OpExpr {
            opno: 0,
            opfuncid: 0,
            op,
            opresulttype: SqlType::new(SqlTypeKind::Bool),
            args: vec![
                Expr::Coalesce(Box::new(int_var()), Box::new(Expr::Const(fallback))),
                Expr::Const(Value::Int32(value)),
            ],
            collation_oid: None,
        }))
    }

    fn coalesce_int8_cmp(op: OpExprKind, value: i64, fallback: Value) -> Expr {
        Expr::Op(Box::new(OpExpr {
            opno: 0,
            opfuncid: 0,
            op,
            opresulttype: SqlType::new(SqlTypeKind::Bool),
            args: vec![
                Expr::Coalesce(Box::new(int8_var()), Box::new(Expr::Const(fallback))),
                Expr::Const(Value::Int64(value)),
            ],
            collation_oid: None,
        }))
    }

    fn coalesce_casted_int_cmp(op: OpExprKind, value: i64) -> Expr {
        Expr::Op(Box::new(OpExpr {
            opno: 0,
            opfuncid: 0,
            op,
            opresulttype: SqlType::new(SqlTypeKind::Bool),
            args: vec![
                Expr::Coalesce(
                    Box::new(int8_var()),
                    Box::new(Expr::Cast(
                        Box::new(Expr::Const(Value::Int32(1))),
                        SqlType::new(SqlTypeKind::Int8),
                    )),
                ),
                Expr::Const(Value::Int64(value)),
            ],
            collation_oid: None,
        }))
    }

    #[test]
    fn numeric_filter_implies_range_partial_predicate() {
        assert!(predicate_implies_index_predicate(
            Some(&int_cmp(OpExprKind::Gt, 1)),
            Some(&int_cmp(OpExprKind::Gt, 0)),
        ));
    }

    #[test]
    fn coalesce_filter_implies_range_partial_predicate() {
        assert!(predicate_implies_index_predicate(
            Some(&coalesce_int_cmp(OpExprKind::Gt, 1, Value::Int32(1))),
            Some(&coalesce_int_cmp(OpExprKind::Gt, 0, Value::Int64(1))),
        ));
    }

    #[test]
    fn coalesce_filter_matches_casted_index_predicate_literals() {
        assert!(predicate_implies_index_predicate(
            Some(&coalesce_int8_cmp(OpExprKind::Gt, 1, Value::Int64(1))),
            Some(&coalesce_casted_int_cmp(OpExprKind::Gt, 0)),
        ));
    }
}
