use super::*;

pub(super) fn expr_contains_agg(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::AggCall { .. } => true,
        SqlExpr::Column(_)
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::ScalarSubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::InSubquery { .. }
        | SqlExpr::QuantifiedSubquery { .. }
        | SqlExpr::Random
        | SqlExpr::FuncCall { .. }
        | SqlExpr::CurrentTimestamp => false,
        SqlExpr::ArrayLiteral(elements) => elements.iter().any(expr_contains_agg),
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
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner) => expr_contains_agg(inner),
    }
}

pub(super) fn targets_contain_agg(targets: &[SelectItem]) -> bool {
    targets.iter().any(|t| expr_contains_agg(&t.expr))
}

pub(super) fn collect_aggs(expr: &SqlExpr, aggs: &mut Vec<(AggFunc, Vec<SqlExpr>, bool)>) {
    match expr {
        SqlExpr::AggCall {
            func,
            args,
            distinct,
        } => {
            let entry = (*func, args.clone(), *distinct);
            if !aggs.contains(&entry) {
                aggs.push(entry);
            }
        }
        SqlExpr::Column(_)
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::ScalarSubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::InSubquery { .. }
        | SqlExpr::QuantifiedSubquery { .. }
        | SqlExpr::Random
        | SqlExpr::CurrentTimestamp => {}
        SqlExpr::FuncCall { args, .. } => {
            for arg in args {
                collect_aggs(arg, aggs);
            }
        }
        SqlExpr::ArrayLiteral(elements) => {
            for element in elements {
                collect_aggs(element, aggs);
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
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner) => collect_aggs(inner, aggs),
    }
}

pub(super) fn sql_expr_name(expr: &SqlExpr) -> String {
    match expr {
        SqlExpr::Column(name) => name.clone(),
        SqlExpr::AggCall { func, .. } => func.name().to_string(),
        SqlExpr::ScalarSubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::InSubquery { .. }
        | SqlExpr::QuantifiedSubquery { .. }
        | SqlExpr::ArrayLiteral(_)
        | SqlExpr::ArrayOverlap(_, _)
        | SqlExpr::QuantifiedArray { .. } => "?column?".to_string(),
        _ => "?column?".to_string(),
    }
}

pub(super) fn ensure_single_column_subquery(plan: &Plan) -> Result<(), ParseError> {
    if plan.columns().len() == 1 {
        Ok(())
    } else {
        Err(ParseError::SubqueryMustReturnOneColumn)
    }
}

pub(super) fn aggregate_sql_type(func: AggFunc, arg_type: Option<SqlType>) -> SqlType {
    use SqlTypeKind::*;

    match func {
        AggFunc::Count => SqlType::new(Int8),
        AggFunc::Sum => match arg_type.map(|t| t.element_type().kind) {
            Some(Int2 | Int4) => SqlType::new(Int8),
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
        AggFunc::Min | AggFunc::Max => arg_type.unwrap_or(SqlType::new(Text)),
        AggFunc::JsonAgg | AggFunc::JsonObjectAgg => SqlType::new(Json),
        AggFunc::JsonbAgg | AggFunc::JsonbObjectAgg => SqlType::new(Jsonb),
    }
}
