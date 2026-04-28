use super::*;
use crate::include::catalog::multirange_type_ref_for_sql_type;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CollectedAggregate {
    pub name: String,
    pub direct_args: Vec<SqlFunctionArg>,
    pub args: SqlCallArgs,
    pub order_by: Vec<OrderByItem>,
    pub distinct: bool,
    pub func_variadic: bool,
    pub filter: Option<SqlExpr>,
}

impl CollectedAggregate {
    pub(super) fn matches_call(
        &self,
        name: &str,
        direct_args: &[SqlFunctionArg],
        args: &SqlCallArgs,
        order_by: &[OrderByItem],
        distinct: bool,
        func_variadic: bool,
        filter: Option<&SqlExpr>,
    ) -> bool {
        self.name.eq_ignore_ascii_case(name)
            && self.direct_args == direct_args
            && self.args == *args
            && self.order_by == order_by
            && self.distinct == distinct
            && self.func_variadic == func_variadic
            && self.filter.as_ref() == filter
    }
}

pub(super) fn aggregate_call_matches_catalog(
    catalog: &dyn CatalogLookup,
    name: &str,
    args: &SqlCallArgs,
    within_group: Option<&[OrderByItem]>,
) -> bool {
    if within_group.is_some() {
        return resolve_builtin_hypothetical_aggregate(name).is_some();
    }
    if let Some(func) = resolve_builtin_aggregate(name) {
        return builtin_aggregate_accepts_call(func, args);
    }

    catalog.proc_rows_by_name(name).into_iter().any(|row| {
        row.prokind == 'a'
            && if args.is_star() {
                row.pronargs == 0
            } else if row.provariadic == 0 {
                row.pronargs as usize == args.args().len()
            } else {
                let fixed = row.pronargs.saturating_sub(1) as usize;
                args.args().len() >= fixed
            }
    })
}

fn builtin_aggregate_accepts_call(func: AggFunc, args: &SqlCallArgs) -> bool {
    let arg_count = args.args().len();
    if args.is_star() {
        return matches!(func, AggFunc::Count);
    }
    match func {
        AggFunc::Count => arg_count <= 1,
        AggFunc::AnyValue
        | AggFunc::Sum
        | AggFunc::Avg
        | AggFunc::VarPop
        | AggFunc::VarSamp
        | AggFunc::StddevPop
        | AggFunc::StddevSamp
        | AggFunc::BoolAnd
        | AggFunc::BoolOr
        | AggFunc::BitAnd
        | AggFunc::BitOr
        | AggFunc::BitXor
        | AggFunc::Min
        | AggFunc::Max
        | AggFunc::ArrayAgg
        | AggFunc::JsonAgg
        | AggFunc::JsonbAgg
        | AggFunc::RangeAgg
        | AggFunc::XmlAgg
        | AggFunc::RangeIntersectAgg => arg_count == 1,
        AggFunc::RegrCount
        | AggFunc::RegrSxx
        | AggFunc::RegrSyy
        | AggFunc::RegrSxy
        | AggFunc::RegrAvgX
        | AggFunc::RegrAvgY
        | AggFunc::RegrR2
        | AggFunc::RegrSlope
        | AggFunc::RegrIntercept
        | AggFunc::CovarPop
        | AggFunc::CovarSamp
        | AggFunc::Corr
        | AggFunc::StringAgg
        | AggFunc::JsonObjectAgg
        | AggFunc::JsonbObjectAgg
        | AggFunc::JsonbObjectAggUnique
        | AggFunc::JsonbObjectAggUniqueStrict => arg_count == 2,
    }
}

pub(super) fn hypothetical_aggregate_args(order_by: &[OrderByItem]) -> SqlCallArgs {
    SqlCallArgs::Args(
        order_by
            .iter()
            .map(|item| SqlFunctionArg::positional(item.expr.clone()))
            .collect(),
    )
}

pub(super) fn normalize_aggregate_call(
    args: &SqlCallArgs,
    order_by: &[OrderByItem],
    within_group: Option<&[OrderByItem]>,
) -> (Vec<SqlFunctionArg>, SqlCallArgs, Vec<OrderByItem>) {
    if let Some(within_group) = within_group {
        (
            args.args().to_vec(),
            hypothetical_aggregate_args(within_group),
            within_group.to_vec(),
        )
    } else {
        (Vec::new(), args.clone(), order_by.to_vec())
    }
}

pub(super) fn expr_contains_agg(catalog: &dyn CatalogLookup, expr: &SqlExpr) -> bool {
    match expr {
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
        | SqlExpr::CurrentCatalog
        | SqlExpr::CurrentSchema
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::CurrentRole
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. } => false,
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            within_group,
            filter,
            over,
            ..
        } => {
            over.is_none()
                && aggregate_call_matches_catalog(catalog, name, args, within_group.as_deref())
                || args
                    .args()
                    .iter()
                    .any(|arg| expr_contains_agg(catalog, &arg.value))
                || order_by
                    .iter()
                    .any(|item| expr_contains_agg(catalog, &item.expr))
                || within_group.as_deref().is_some_and(|items| {
                    items
                        .iter()
                        .any(|item| expr_contains_agg(catalog, &item.expr))
                })
                || filter
                    .as_deref()
                    .is_some_and(|expr| expr_contains_agg(catalog, expr))
        }
        SqlExpr::ArrayLiteral(elements) | SqlExpr::Row(elements) => {
            elements.iter().any(|expr| expr_contains_agg(catalog, expr))
        }
        SqlExpr::BinaryOperator { left, right, .. } => {
            expr_contains_agg(catalog, left) || expr_contains_agg(catalog, right)
        }
        SqlExpr::PrefixOperator { expr, .. } | SqlExpr::FieldSelect { expr, .. } => {
            expr_contains_agg(catalog, expr)
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            expr_contains_agg(catalog, array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_deref()
                        .is_some_and(|expr| expr_contains_agg(catalog, expr))
                        || subscript
                            .upper
                            .as_deref()
                            .is_some_and(|expr| expr_contains_agg(catalog, expr))
                })
        }
        SqlExpr::ArrayOverlap(l, r)
        | SqlExpr::ArrayContains(l, r)
        | SqlExpr::ArrayContained(l, r)
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
            expr_contains_agg(catalog, l) || expr_contains_agg(catalog, r)
        }
        SqlExpr::Cast(inner, _) | SqlExpr::Collate { expr: inner, .. } => {
            expr_contains_agg(catalog, inner)
        }
        SqlExpr::AtTimeZone { expr, zone } => {
            expr_contains_agg(catalog, expr) || expr_contains_agg(catalog, zone)
        }
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
        | SqlExpr::IsNotDistinctFrom(l, r)
        | SqlExpr::Overlaps(l, r) => expr_contains_agg(catalog, l) || expr_contains_agg(catalog, r),
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_agg(catalog, expr)
                || expr_contains_agg(catalog, pattern)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_contains_agg(catalog, expr))
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            arg.as_deref()
                .is_some_and(|expr| expr_contains_agg(catalog, expr))
                || args.iter().any(|arm| {
                    expr_contains_agg(catalog, &arm.expr) || expr_contains_agg(catalog, &arm.result)
                })
                || defresult
                    .as_deref()
                    .is_some_and(|expr| expr_contains_agg(catalog, expr))
        }
        SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_agg(catalog, expr)
                || expr_contains_agg(catalog, pattern)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_contains_agg(catalog, expr))
        }
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::GeometryUnaryOp { expr: inner, .. }
        | SqlExpr::Subscript { expr: inner, .. } => expr_contains_agg(catalog, inner),
        SqlExpr::GeometryBinaryOp { left, right, .. } => {
            expr_contains_agg(catalog, left) || expr_contains_agg(catalog, right)
        }
        SqlExpr::Xml(xml) => xml
            .child_exprs()
            .any(|expr| expr_contains_agg(catalog, expr)),
        SqlExpr::JsonQueryFunction(func) => func
            .child_exprs()
            .iter()
            .any(|expr| expr_contains_agg(catalog, expr)),
    }
}

pub(super) fn targets_contain_agg(catalog: &dyn CatalogLookup, targets: &[SelectItem]) -> bool {
    targets
        .iter()
        .any(|target| expr_contains_agg(catalog, &target.expr))
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
        | SqlExpr::CurrentCatalog
        | SqlExpr::CurrentSchema
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
        SqlExpr::FuncCall {
            args,
            order_by,
            within_group,
            filter,
            ..
        } => {
            args.args()
                .iter()
                .any(|arg| expr_references_input_scope(&arg.value))
                || order_by
                    .iter()
                    .any(|item| expr_references_input_scope(&item.expr))
                || within_group.as_deref().is_some_and(|items| {
                    items
                        .iter()
                        .any(|item| expr_references_input_scope(&item.expr))
                })
                || filter.as_deref().is_some_and(expr_references_input_scope)
        }
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
        | SqlExpr::ArrayContains(l, r)
        | SqlExpr::ArrayContained(l, r)
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
        | SqlExpr::IsNotDistinctFrom(l, r)
        | SqlExpr::Overlaps(l, r) => {
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
        | SqlExpr::Collate { expr: inner, .. }
        | SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::GeometryUnaryOp { expr: inner, .. }
        | SqlExpr::Subscript { expr: inner, .. } => expr_references_input_scope(inner),
        SqlExpr::AtTimeZone { expr, zone } => {
            expr_references_input_scope(expr) || expr_references_input_scope(zone)
        }
        SqlExpr::GeometryBinaryOp { left, right, .. } => {
            expr_references_input_scope(left) || expr_references_input_scope(right)
        }
        SqlExpr::Xml(xml) => xml.child_exprs().any(expr_references_input_scope),
        SqlExpr::JsonQueryFunction(func) => func
            .child_exprs()
            .iter()
            .any(|expr| expr_references_input_scope(expr)),
    }
}

pub(super) fn collect_aggs(
    catalog: &dyn CatalogLookup,
    expr: &SqlExpr,
    aggs: &mut Vec<CollectedAggregate>,
) {
    match expr {
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
        | SqlExpr::CurrentCatalog
        | SqlExpr::CurrentSchema
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::CurrentRole
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. } => {}
        SqlExpr::BinaryOperator { left, right, .. } => {
            collect_aggs(catalog, left, aggs);
            collect_aggs(catalog, right, aggs);
        }
        SqlExpr::PrefixOperator { expr, .. } | SqlExpr::FieldSelect { expr, .. } => {
            collect_aggs(catalog, expr, aggs);
        }
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            within_group,
            distinct,
            func_variadic,
            filter,
            over,
            ..
        } => {
            if aggregate_call_matches_catalog(catalog, name, args, within_group.as_deref()) {
                if over.is_none() {
                    let (direct_args, agg_args, agg_order_by) =
                        if let Some(within_group) = within_group.as_deref() {
                            (
                                args.args().to_vec(),
                                hypothetical_aggregate_args(within_group),
                                within_group.to_vec(),
                            )
                        } else {
                            (Vec::new(), args.clone(), order_by.clone())
                        };
                    let entry = CollectedAggregate {
                        name: name.clone(),
                        direct_args,
                        args: agg_args,
                        order_by: agg_order_by,
                        distinct: *distinct,
                        func_variadic: *func_variadic,
                        filter: filter.as_deref().cloned(),
                    };
                    if !aggs.contains(&entry) {
                        aggs.push(entry);
                    }
                }
            }
            for arg in args.args() {
                collect_aggs(catalog, &arg.value, aggs);
            }
            for item in order_by {
                collect_aggs(catalog, &item.expr, aggs);
            }
            if let Some(items) = within_group.as_deref() {
                for item in items {
                    collect_aggs(catalog, &item.expr, aggs);
                }
            }
            if let Some(filter) = filter.as_deref() {
                collect_aggs(catalog, filter, aggs);
            }
        }
        SqlExpr::ArrayLiteral(elements) | SqlExpr::Row(elements) => {
            for element in elements {
                collect_aggs(catalog, element, aggs);
            }
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            collect_aggs(catalog, array, aggs);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_aggs(catalog, lower, aggs);
                }
                if let Some(upper) = &subscript.upper {
                    collect_aggs(catalog, upper, aggs);
                }
            }
        }
        SqlExpr::ArrayOverlap(l, r)
        | SqlExpr::ArrayContains(l, r)
        | SqlExpr::ArrayContained(l, r)
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
            collect_aggs(catalog, l, aggs);
            collect_aggs(catalog, r, aggs);
        }
        SqlExpr::Cast(inner, _) | SqlExpr::Collate { expr: inner, .. } => {
            collect_aggs(catalog, inner, aggs)
        }
        SqlExpr::AtTimeZone { expr, zone } => {
            collect_aggs(catalog, expr, aggs);
            collect_aggs(catalog, zone, aggs);
        }
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
        | SqlExpr::IsNotDistinctFrom(l, r)
        | SqlExpr::Overlaps(l, r) => {
            collect_aggs(catalog, l, aggs);
            collect_aggs(catalog, r, aggs);
        }
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_aggs(catalog, expr, aggs);
            collect_aggs(catalog, pattern, aggs);
            if let Some(escape) = escape {
                collect_aggs(catalog, escape, aggs);
            }
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            if let Some(arg) = arg {
                collect_aggs(catalog, arg, aggs);
            }
            for arm in args {
                collect_aggs(catalog, &arm.expr, aggs);
                collect_aggs(catalog, &arm.result, aggs);
            }
            if let Some(defresult) = defresult {
                collect_aggs(catalog, defresult, aggs);
            }
        }
        SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_aggs(catalog, expr, aggs);
            collect_aggs(catalog, pattern, aggs);
            if let Some(escape) = escape {
                collect_aggs(catalog, escape, aggs);
            }
        }
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::GeometryUnaryOp { expr: inner, .. }
        | SqlExpr::Subscript { expr: inner, .. } => collect_aggs(catalog, inner, aggs),
        SqlExpr::GeometryBinaryOp { left, right, .. } => {
            collect_aggs(catalog, left, aggs);
            collect_aggs(catalog, right, aggs);
        }
        SqlExpr::Xml(xml) => {
            for child in xml.child_exprs() {
                collect_aggs(catalog, child, aggs);
            }
        }
        SqlExpr::JsonQueryFunction(func) => {
            for child in func.child_exprs() {
                collect_aggs(catalog, child, aggs);
            }
        }
    }
}

pub(super) fn sql_expr_name(expr: &SqlExpr) -> String {
    match expr {
        SqlExpr::Column(name) => name.clone(),
        SqlExpr::FuncCall { name, .. } => name.clone(),
        SqlExpr::ScalarSubquery(select) => select
            .targets
            .first()
            .map(|target| target.output_name.clone())
            .unwrap_or_else(|| "?column?".to_string()),
        SqlExpr::ArraySubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::InSubquery { .. }
        | SqlExpr::QuantifiedSubquery { .. }
        | SqlExpr::ArrayLiteral(_)
        | SqlExpr::Overlaps(_, _)
        | SqlExpr::ArrayOverlap(_, _)
        | SqlExpr::ArrayContains(_, _)
        | SqlExpr::ArrayContained(_, _)
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
        AggFunc::AnyValue => arg_type.unwrap_or(SqlType::new(Text)),
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
        AggFunc::VarPop | AggFunc::VarSamp | AggFunc::StddevPop | AggFunc::StddevSamp => {
            match arg_type.map(|t| t.element_type().kind) {
                Some(Int2 | Int4 | Int8 | Numeric) => SqlType::new(Numeric),
                Some(Float4 | Float8) => SqlType::new(Float8),
                Some(kind) => SqlType::new(kind),
                None => SqlType::new(Numeric),
            }
        }
        AggFunc::RegrCount => SqlType::new(Int8),
        AggFunc::RegrSxx
        | AggFunc::RegrSyy
        | AggFunc::RegrSxy
        | AggFunc::RegrAvgX
        | AggFunc::RegrAvgY
        | AggFunc::RegrR2
        | AggFunc::RegrSlope
        | AggFunc::RegrIntercept
        | AggFunc::CovarPop
        | AggFunc::CovarSamp
        | AggFunc::Corr => SqlType::new(Float8),
        AggFunc::BoolAnd | AggFunc::BoolOr => SqlType::new(Bool),
        AggFunc::BitAnd | AggFunc::BitOr | AggFunc::BitXor => {
            arg_type.unwrap_or(SqlType::new(Int4))
        }
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
        AggFunc::XmlAgg => SqlType::new(Xml),
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
        | AggFunc::JsonbObjectAgg
        | AggFunc::JsonbObjectAggUnique
        | AggFunc::JsonbObjectAggUniqueStrict => {
            unreachable!("fixed aggregate return types handled above")
        }
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
