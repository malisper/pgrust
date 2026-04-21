use super::*;
use crate::include::catalog::multirange_type_ref_for_sql_type;

pub(super) fn expr_contains_agg(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::AggCall { over, .. } => over.is_none(),
        SqlExpr::Column(_)
        | SqlExpr::Default
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::ScalarSubquery(_)
        | SqlExpr::ArraySubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::InSubquery { .. }
        | SqlExpr::QuantifiedSubquery { .. }
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::CurrentRole
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. } => false,
        SqlExpr::FuncCall { args, .. } => args.iter().any(|arg| expr_contains_agg(&arg.value)),
        SqlExpr::ArrayLiteral(elements) | SqlExpr::Row(elements) => {
            elements.iter().any(expr_contains_agg)
        }
        SqlExpr::BinaryOperator { left, right, .. } => {
            expr_contains_agg(left) || expr_contains_agg(right)
        }
        SqlExpr::PrefixOperator { expr, .. } | SqlExpr::FieldSelect { expr, .. } => {
            expr_contains_agg(expr)
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            expr_contains_agg(array)
                || subscripts.iter().any(|subscript| {
                    subscript.lower.as_deref().is_some_and(expr_contains_agg)
                        || subscript.upper.as_deref().is_some_and(expr_contains_agg)
                })
        }
        SqlExpr::ArrayOverlap(l, r)
        | SqlExpr::QuantifiedArray {
            left: l, array: r, ..
        }
        | SqlExpr::JsonGet(l, r)
        | SqlExpr::JsonGetText(l, r)
        | SqlExpr::JsonPath(l, r)
        | SqlExpr::JsonPathText(l, r)
        | SqlExpr::JsonbContains(l, r)
        | SqlExpr::JsonbContained(l, r)
        | SqlExpr::JsonbExists(l, r)
        | SqlExpr::JsonbExistsAny(l, r)
        | SqlExpr::JsonbExistsAll(l, r)
        | SqlExpr::JsonbPathExists(l, r)
        | SqlExpr::JsonbPathMatch(l, r) => expr_contains_agg(l) || expr_contains_agg(r),
        SqlExpr::Cast(inner, _) => expr_contains_agg(inner),
        SqlExpr::Add(l, r)
        | SqlExpr::Sub(l, r)
        | SqlExpr::BitAnd(l, r)
        | SqlExpr::BitOr(l, r)
        | SqlExpr::BitXor(l, r)
        | SqlExpr::Shl(l, r)
        | SqlExpr::Shr(l, r)
        | SqlExpr::Mul(l, r)
        | SqlExpr::Div(l, r)
        | SqlExpr::Mod(l, r)
        | SqlExpr::Concat(l, r)
        | SqlExpr::Eq(l, r)
        | SqlExpr::NotEq(l, r)
        | SqlExpr::Lt(l, r)
        | SqlExpr::LtEq(l, r)
        | SqlExpr::Gt(l, r)
        | SqlExpr::GtEq(l, r)
        | SqlExpr::RegexMatch(l, r)
        | SqlExpr::And(l, r)
        | SqlExpr::Or(l, r)
        | SqlExpr::IsDistinctFrom(l, r)
        | SqlExpr::IsNotDistinctFrom(l, r) => expr_contains_agg(l) || expr_contains_agg(r),
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_agg(expr)
                || expr_contains_agg(pattern)
                || escape.as_ref().is_some_and(|e| expr_contains_agg(e))
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            arg.as_deref().is_some_and(expr_contains_agg)
                || args
                    .iter()
                    .any(|arm| expr_contains_agg(&arm.expr) || expr_contains_agg(&arm.result))
                || defresult.as_deref().is_some_and(expr_contains_agg)
        }
        SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_agg(expr)
                || expr_contains_agg(pattern)
                || escape.as_ref().is_some_and(|e| expr_contains_agg(e))
        }
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::GeometryUnaryOp { expr: inner, .. }
        | SqlExpr::Subscript { expr: inner, .. } => expr_contains_agg(inner),
        SqlExpr::GeometryBinaryOp { left, right, .. } => {
            expr_contains_agg(left) || expr_contains_agg(right)
        }
    }
}

pub(super) fn targets_contain_agg(targets: &[SelectItem]) -> bool {
    targets.iter().any(|t| expr_contains_agg(&t.expr))
}

pub(super) fn expr_references_input_scope(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Column(_) => true,
        SqlExpr::Default => false,
        SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::CurrentRole
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. } => false,
        SqlExpr::BinaryOperator { left, right, .. } => {
            expr_references_input_scope(left) || expr_references_input_scope(right)
        }
        SqlExpr::AggCall {
            args,
            order_by,
            filter,
            ..
        } => {
            args.iter()
                .any(|arg| expr_references_input_scope(&arg.value))
                || order_by
                    .iter()
                    .any(|item| expr_references_input_scope(&item.expr))
                || filter.as_deref().is_some_and(expr_references_input_scope)
        }
        SqlExpr::FuncCall { args, .. } => args
            .iter()
            .any(|arg| expr_references_input_scope(&arg.value)),
        SqlExpr::PrefixOperator { expr, .. } | SqlExpr::FieldSelect { expr, .. } => {
            expr_references_input_scope(expr)
        }
        SqlExpr::ArrayLiteral(elements) | SqlExpr::Row(elements) => {
            elements.iter().any(expr_references_input_scope)
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            expr_references_input_scope(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_deref()
                        .is_some_and(expr_references_input_scope)
                        || subscript
                            .upper
                            .as_deref()
                            .is_some_and(expr_references_input_scope)
                })
        }
        SqlExpr::ScalarSubquery(_)
        | SqlExpr::ArraySubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::InSubquery { .. }
        | SqlExpr::QuantifiedSubquery { .. } => true,
        SqlExpr::ArrayOverlap(l, r)
        | SqlExpr::QuantifiedArray {
            left: l, array: r, ..
        }
        | SqlExpr::JsonGet(l, r)
        | SqlExpr::JsonGetText(l, r)
        | SqlExpr::JsonPath(l, r)
        | SqlExpr::JsonPathText(l, r)
        | SqlExpr::JsonbContains(l, r)
        | SqlExpr::JsonbContained(l, r)
        | SqlExpr::JsonbExists(l, r)
        | SqlExpr::JsonbExistsAny(l, r)
        | SqlExpr::JsonbExistsAll(l, r)
        | SqlExpr::JsonbPathExists(l, r)
        | SqlExpr::JsonbPathMatch(l, r)
        | SqlExpr::Add(l, r)
        | SqlExpr::Sub(l, r)
        | SqlExpr::BitAnd(l, r)
        | SqlExpr::BitOr(l, r)
        | SqlExpr::BitXor(l, r)
        | SqlExpr::Shl(l, r)
        | SqlExpr::Shr(l, r)
        | SqlExpr::Mul(l, r)
        | SqlExpr::Div(l, r)
        | SqlExpr::Mod(l, r)
        | SqlExpr::Concat(l, r)
        | SqlExpr::Eq(l, r)
        | SqlExpr::NotEq(l, r)
        | SqlExpr::Lt(l, r)
        | SqlExpr::LtEq(l, r)
        | SqlExpr::Gt(l, r)
        | SqlExpr::GtEq(l, r)
        | SqlExpr::RegexMatch(l, r)
        | SqlExpr::And(l, r)
        | SqlExpr::Or(l, r)
        | SqlExpr::IsDistinctFrom(l, r)
        | SqlExpr::IsNotDistinctFrom(l, r) => {
            expr_references_input_scope(l) || expr_references_input_scope(r)
        }
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_references_input_scope(expr)
                || expr_references_input_scope(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|e| expr_references_input_scope(e))
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            arg.as_deref().is_some_and(expr_references_input_scope)
                || args.iter().any(|arm| {
                    expr_references_input_scope(&arm.expr)
                        || expr_references_input_scope(&arm.result)
                })
                || defresult
                    .as_deref()
                    .is_some_and(expr_references_input_scope)
        }
        SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_references_input_scope(expr)
                || expr_references_input_scope(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|e| expr_references_input_scope(e))
        }
        SqlExpr::Cast(inner, _)
        | SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::GeometryUnaryOp { expr: inner, .. }
        | SqlExpr::Subscript { expr: inner, .. } => expr_references_input_scope(inner),
        SqlExpr::GeometryBinaryOp { left, right, .. } => {
            expr_references_input_scope(left) || expr_references_input_scope(right)
        }
    }
}

pub(super) fn collect_aggs(
    expr: &SqlExpr,
    aggs: &mut Vec<(
        AggFunc,
        Vec<SqlFunctionArg>,
        Vec<OrderByItem>,
        bool,
        bool,
        Option<SqlExpr>,
    )>,
) {
    match expr {
        SqlExpr::AggCall {
            func,
            args,
            order_by,
            distinct,
            func_variadic,
            filter,
            over,
        } => {
            if over.is_some() {
                for arg in args {
                    collect_aggs(&arg.value, aggs);
                }
                for item in order_by {
                    collect_aggs(&item.expr, aggs);
                }
                if let Some(filter) = filter.as_deref() {
                    collect_aggs(filter, aggs);
                }
                return;
            }
            let entry = (
                *func,
                args.clone(),
                order_by.clone(),
                *distinct,
                *func_variadic,
                filter.as_deref().cloned(),
            );
            if !aggs.contains(&entry) {
                aggs.push(entry);
            }
        }
        SqlExpr::Column(_)
        | SqlExpr::Default
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::ScalarSubquery(_)
        | SqlExpr::ArraySubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::InSubquery { .. }
        | SqlExpr::QuantifiedSubquery { .. }
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::CurrentRole
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. } => {}
        SqlExpr::BinaryOperator { left, right, .. } => {
            collect_aggs(left, aggs);
            collect_aggs(right, aggs);
        }
        SqlExpr::PrefixOperator { expr, .. } | SqlExpr::FieldSelect { expr, .. } => {
            collect_aggs(expr, aggs);
        }
        SqlExpr::FuncCall { args, .. } => {
            for arg in args {
                collect_aggs(&arg.value, aggs);
            }
        }
        SqlExpr::ArrayLiteral(elements) | SqlExpr::Row(elements) => {
            for element in elements {
                collect_aggs(element, aggs);
            }
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            collect_aggs(array, aggs);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_aggs(lower, aggs);
                }
                if let Some(upper) = &subscript.upper {
                    collect_aggs(upper, aggs);
                }
            }
        }
        SqlExpr::ArrayOverlap(l, r)
        | SqlExpr::QuantifiedArray {
            left: l, array: r, ..
        }
        | SqlExpr::JsonGet(l, r)
        | SqlExpr::JsonGetText(l, r)
        | SqlExpr::JsonPath(l, r)
        | SqlExpr::JsonPathText(l, r)
        | SqlExpr::JsonbContains(l, r)
        | SqlExpr::JsonbContained(l, r)
        | SqlExpr::JsonbExists(l, r)
        | SqlExpr::JsonbExistsAny(l, r)
        | SqlExpr::JsonbExistsAll(l, r)
        | SqlExpr::JsonbPathExists(l, r)
        | SqlExpr::JsonbPathMatch(l, r) => {
            collect_aggs(l, aggs);
            collect_aggs(r, aggs);
        }
        SqlExpr::Cast(inner, _) => collect_aggs(inner, aggs),
        SqlExpr::Add(l, r)
        | SqlExpr::Sub(l, r)
        | SqlExpr::BitAnd(l, r)
        | SqlExpr::BitOr(l, r)
        | SqlExpr::BitXor(l, r)
        | SqlExpr::Shl(l, r)
        | SqlExpr::Shr(l, r)
        | SqlExpr::Mul(l, r)
        | SqlExpr::Div(l, r)
        | SqlExpr::Mod(l, r)
        | SqlExpr::Concat(l, r)
        | SqlExpr::Eq(l, r)
        | SqlExpr::NotEq(l, r)
        | SqlExpr::Lt(l, r)
        | SqlExpr::LtEq(l, r)
        | SqlExpr::Gt(l, r)
        | SqlExpr::GtEq(l, r)
        | SqlExpr::RegexMatch(l, r)
        | SqlExpr::And(l, r)
        | SqlExpr::Or(l, r)
        | SqlExpr::IsDistinctFrom(l, r)
        | SqlExpr::IsNotDistinctFrom(l, r) => {
            collect_aggs(l, aggs);
            collect_aggs(r, aggs);
        }
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_aggs(expr, aggs);
            collect_aggs(pattern, aggs);
            if let Some(escape) = escape {
                collect_aggs(escape, aggs);
            }
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            if let Some(arg) = arg {
                collect_aggs(arg, aggs);
            }
            for arm in args {
                collect_aggs(&arm.expr, aggs);
                collect_aggs(&arm.result, aggs);
            }
            if let Some(defresult) = defresult {
                collect_aggs(defresult, aggs);
            }
        }
        SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_aggs(expr, aggs);
            collect_aggs(pattern, aggs);
            if let Some(escape) = escape {
                collect_aggs(escape, aggs);
            }
        }
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::GeometryUnaryOp { expr: inner, .. }
        | SqlExpr::Subscript { expr: inner, .. } => collect_aggs(inner, aggs),
        SqlExpr::GeometryBinaryOp { left, right, .. } => {
            collect_aggs(left, aggs);
            collect_aggs(right, aggs);
        }
    }
}

pub(super) fn sql_expr_name(expr: &SqlExpr) -> String {
    match expr {
        SqlExpr::Column(name) => name.clone(),
        SqlExpr::AggCall { func, .. } => func.name().to_string(),
        SqlExpr::ScalarSubquery(_)
        | SqlExpr::ArraySubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::InSubquery { .. }
        | SqlExpr::QuantifiedSubquery { .. }
        | SqlExpr::ArrayLiteral(_)
        | SqlExpr::ArrayOverlap(_, _)
        | SqlExpr::QuantifiedArray { .. } => "?column?".to_string(),
        _ => "?column?".to_string(),
    }
}

pub(super) fn ensure_single_column_subquery(width: usize) -> Result<(), ParseError> {
    if width == 1 {
        Ok(())
    } else {
        Err(ParseError::SubqueryMustReturnOneColumn)
    }
}

pub(super) fn aggregate_sql_type(func: AggFunc, arg_type: Option<SqlType>) -> SqlType {
    if let Some(sql_type) = fixed_aggregate_return_type(func) {
        return sql_type;
    }

    use SqlTypeKind::*;

    match func {
        AggFunc::Sum => match arg_type.map(|t| t.element_type().kind) {
            Some(Int2 | Int4) => SqlType::new(Int8),
            Some(Money) => SqlType::new(Money),
            Some(Int8 | Numeric) => SqlType::new(Numeric),
            Some(Float4) => SqlType::new(Float4),
            Some(Float8) => SqlType::new(Float8),
            Some(kind) => SqlType::new(kind),
            None => SqlType::new(Int8),
        },
        AggFunc::Avg => match arg_type.map(|t| t.element_type().kind) {
            Some(Int2 | Int4 | Int8 | Numeric) => SqlType::new(Numeric),
            Some(Float4 | Float8) => SqlType::new(Float8),
            Some(kind) => SqlType::new(kind),
            None => SqlType::new(Numeric),
        },
        AggFunc::Variance | AggFunc::Stddev => match arg_type.map(|t| t.element_type().kind) {
            Some(Int2 | Int4 | Int8 | Numeric) => SqlType::new(Numeric),
            Some(Float4 | Float8) => SqlType::new(Float8),
            Some(kind) => SqlType::new(kind),
            None => SqlType::new(Numeric),
        },
        AggFunc::ArrayAgg => arg_type
            .map(|ty| {
                if ty.is_array {
                    ty
                } else {
                    SqlType::array_of(ty)
                }
            })
            .unwrap_or(SqlType::array_of(SqlType::new(Text))),
        AggFunc::StringAgg => arg_type.unwrap_or(SqlType::new(Text)),
        AggFunc::Min | AggFunc::Max => arg_type.unwrap_or(SqlType::new(Text)),
        AggFunc::RangeAgg => arg_type
            .and_then(|ty| {
                if ty.is_multirange() {
                    Some(ty)
                } else {
                    multirange_type_ref_for_sql_type(SqlType::multirange(
                        ty.range_multitype_oid,
                        ty.type_oid,
                    ))
                    .map(|multirange_type| multirange_type.sql_type)
                }
            })
            .unwrap_or(SqlType::new(Text)),
        AggFunc::Count
        | AggFunc::JsonAgg
        | AggFunc::JsonbAgg
        | AggFunc::JsonObjectAgg
        | AggFunc::JsonbObjectAgg => unreachable!("fixed aggregate return types handled above"),
        AggFunc::RangeIntersectAgg => arg_type.unwrap_or(SqlType::new(Text)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_sql_type_uses_pg_proc_for_fixed_return_aggs() {
        assert_eq!(
            aggregate_sql_type(AggFunc::Count, Some(SqlType::new(SqlTypeKind::Int4))),
            SqlType::new(SqlTypeKind::Int8)
        );
        assert_eq!(
            aggregate_sql_type(AggFunc::JsonAgg, Some(SqlType::new(SqlTypeKind::Text))),
            SqlType::new(SqlTypeKind::Json)
        );
        assert_eq!(
            aggregate_sql_type(
                AggFunc::JsonbObjectAgg,
                Some(SqlType::new(SqlTypeKind::Text)),
            ),
            SqlType::new(SqlTypeKind::Jsonb)
        );
        assert_eq!(
            aggregate_sql_type(AggFunc::Sum, Some(SqlType::new(SqlTypeKind::Int4))),
            SqlType::new(SqlTypeKind::Int8)
        );
        assert_eq!(
            aggregate_sql_type(AggFunc::StringAgg, Some(SqlType::new(SqlTypeKind::Bytea))),
            SqlType::new(SqlTypeKind::Bytea)
        );
    }
}
