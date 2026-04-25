use super::functions::{resolve_function_call, resolve_scalar_function};
use super::multiranges::infer_multirange_special_expr_type_with_ctes;
use super::ranges::infer_range_special_expr_type_with_ctes;
use super::*;
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::include::catalog::range_type_ref_for_sql_type;
use crate::include::nodes::primnodes::expr_sql_type_hint;

fn array_subscript_element_type(array_type: SqlType) -> SqlType {
    if array_type.is_array {
        return array_type.element_type();
    }
    match array_type.kind {
        SqlTypeKind::Jsonb => SqlType::new(SqlTypeKind::Jsonb),
        SqlTypeKind::Point => SqlType::new(SqlTypeKind::Float8),
        SqlTypeKind::Int2Vector => SqlType::new(SqlTypeKind::Int2),
        SqlTypeKind::OidVector => SqlType::new(SqlTypeKind::Oid),
        _ => array_type.element_type(),
    }
}

pub(super) fn infer_sql_expr_type(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> SqlType {
    infer_sql_expr_type_with_ctes(expr, scope, catalog, outer_scopes, grouped_outer, &[])
}

fn child_outer_scopes(scope: &BoundScope, outer_scopes: &[BoundScope]) -> Vec<BoundScope> {
    let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
    child_outer.push(scope.clone());
    child_outer.extend_from_slice(outer_scopes);
    child_outer
}

fn infer_relation_row_expr_type(
    scope: &BoundScope,
    outer_scopes: &[BoundScope],
    name: &str,
) -> Option<SqlType> {
    resolve_relation_row_expr_with_outer(scope, outer_scopes, name).map(|fields| {
        assign_anonymous_record_descriptor(
            fields
                .iter()
                .map(|(field_name, field_expr)| {
                    (
                        field_name.clone(),
                        expr_sql_type_hint(field_expr).unwrap_or(SqlType::new(SqlTypeKind::Text)),
                    )
                })
                .collect(),
        )
        .sql_type()
    })
}

fn infer_visible_outer_aggregate_type(
    name: &str,
    direct_args: &[SqlFunctionArg],
    args: &SqlCallArgs,
    order_by: &[OrderByItem],
    distinct: bool,
    func_variadic: bool,
    filter: Option<&SqlExpr>,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    ctes: &[BoundCte],
) -> Option<SqlType> {
    let (_, visible_scope) = match_visible_aggregate_call(
        name,
        direct_args,
        args,
        order_by,
        distinct,
        func_variadic,
        filter,
        catalog,
        outer_scopes,
        ctes,
    )?;
    let owner_scope = &visible_scope.input_scope;
    let owner_outer_scopes = outer_scopes.get(visible_scope.levelsup..).unwrap_or(&[]);
    let arg_types = args
        .args()
        .iter()
        .map(|arg| {
            infer_sql_expr_type_with_ctes(
                &arg.value,
                owner_scope,
                catalog,
                owner_outer_scopes,
                None,
                ctes,
            )
        })
        .collect::<Vec<_>>();
    if !direct_args.is_empty() {
        resolve_hypothetical_aggregate_call(name).map(|resolved| resolved.result_type)
    } else {
        resolve_aggregate_call(catalog, name, &arg_types, func_variadic)
            .map(|resolved| resolved.result_type)
    }
}

pub(super) fn infer_sql_expr_type_with_ctes(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> SqlType {
    if matches_grouped_outer_expr(expr, grouped_outer) {
        return infer_sql_expr_type_with_ctes(expr, scope, catalog, outer_scopes, None, ctes);
    }

    if let Some(sql_type) = infer_geometry_special_expr_type_with_ctes(
        expr,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ) {
        return sql_type;
    }

    if let Some(sql_type) = infer_range_special_expr_type_with_ctes(
        expr,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ) {
        return sql_type;
    }

    if let Some(sql_type) = infer_multirange_special_expr_type_with_ctes(
        expr,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    ) {
        return sql_type;
    }

    match expr {
        SqlExpr::Column(name) => {
            if let Some(relation_name) = name.strip_suffix(".*") {
                infer_relation_row_expr_type(scope, outer_scopes, relation_name)
                    .unwrap_or(SqlType::new(SqlTypeKind::Text))
            } else {
                resolve_system_column_with_outer(scope, outer_scopes, name)
                    .ok()
                    .flatten()
                    .map(|resolved| resolved.sql_type)
                    .or_else(|| {
                        match resolve_column_with_outer(scope, outer_scopes, name, grouped_outer) {
                            Ok(ResolvedColumn::Local(idx)) => {
                                scope.desc.columns.get(idx).map(|c| c.sql_type)
                            }
                            Ok(ResolvedColumn::Outer { depth, index }) => outer_scopes
                                .get(depth)
                                .and_then(|s| s.desc.columns.get(index).map(|c| c.sql_type)),
                            Err(ParseError::UnknownColumn(_)) => {
                                infer_relation_row_expr_type(scope, outer_scopes, name)
                            }
                            Err(_) => None,
                        }
                    })
                    .unwrap_or(SqlType::new(SqlTypeKind::Text))
            }
        }
        SqlExpr::Default => SqlType::new(SqlTypeKind::Text),
        SqlExpr::Const(Value::Int16(_)) => SqlType::new(SqlTypeKind::Int2),
        SqlExpr::Const(Value::Int32(_)) => SqlType::new(SqlTypeKind::Int4),
        SqlExpr::Const(Value::Int64(_)) => SqlType::new(SqlTypeKind::Int8),
        SqlExpr::Const(Value::Money(_)) => SqlType::new(SqlTypeKind::Money),
        SqlExpr::Const(Value::Date(_)) => SqlType::new(SqlTypeKind::Date),
        SqlExpr::Const(Value::Time(_)) => SqlType::new(SqlTypeKind::Time),
        SqlExpr::Const(Value::TimeTz(_)) => SqlType::new(SqlTypeKind::TimeTz),
        SqlExpr::Const(Value::Timestamp(_)) => SqlType::new(SqlTypeKind::Timestamp),
        SqlExpr::Const(Value::TimestampTz(_)) => SqlType::new(SqlTypeKind::TimestampTz),
        SqlExpr::Const(Value::Interval(_)) => SqlType::new(SqlTypeKind::Interval),
        SqlExpr::Const(Value::Range(range)) => range.range_type.sql_type,
        SqlExpr::Const(Value::Multirange(multirange)) => multirange.multirange_type.sql_type,
        SqlExpr::Const(Value::Bit(v)) => SqlType::with_bit_len(SqlTypeKind::VarBit, v.bit_len),
        SqlExpr::Const(Value::Bytea(_)) => SqlType::new(SqlTypeKind::Bytea),
        SqlExpr::Const(Value::Uuid(_)) => SqlType::new(SqlTypeKind::Uuid),
        SqlExpr::Const(Value::Inet(_)) => SqlType::new(SqlTypeKind::Inet),
        SqlExpr::Const(Value::Cidr(_)) => SqlType::new(SqlTypeKind::Cidr),
        SqlExpr::Const(Value::MacAddr(_)) => SqlType::new(SqlTypeKind::MacAddr),
        SqlExpr::Const(Value::MacAddr8(_)) => SqlType::new(SqlTypeKind::MacAddr8),
        SqlExpr::Const(Value::Bool(_)) => SqlType::new(SqlTypeKind::Bool),
        SqlExpr::Row(items) => {
            infer_sql_row_expr_type(items, scope, catalog, outer_scopes, grouped_outer, ctes)
        }
        SqlExpr::Const(Value::Numeric(_)) => SqlType::new(SqlTypeKind::Numeric),
        SqlExpr::Const(Value::Json(_)) => SqlType::new(SqlTypeKind::Json),
        SqlExpr::Const(Value::Jsonb(_)) => SqlType::new(SqlTypeKind::Jsonb),
        SqlExpr::Const(Value::JsonPath(_)) => SqlType::new(SqlTypeKind::JsonPath),
        SqlExpr::Const(Value::Xml(_)) => SqlType::new(SqlTypeKind::Xml),
        SqlExpr::Const(Value::Point(_)) => SqlType::new(SqlTypeKind::Point),
        SqlExpr::Const(Value::Lseg(_)) => SqlType::new(SqlTypeKind::Lseg),
        SqlExpr::Const(Value::Path(_)) => SqlType::new(SqlTypeKind::Path),
        SqlExpr::Const(Value::Line(_)) => SqlType::new(SqlTypeKind::Line),
        SqlExpr::Const(Value::Box(_)) => SqlType::new(SqlTypeKind::Box),
        SqlExpr::Const(Value::Polygon(_)) => SqlType::new(SqlTypeKind::Polygon),
        SqlExpr::Const(Value::Circle(_)) => SqlType::new(SqlTypeKind::Circle),
        SqlExpr::Const(Value::TsVector(_)) => SqlType::new(SqlTypeKind::TsVector),
        SqlExpr::Const(Value::TsQuery(_)) => SqlType::new(SqlTypeKind::TsQuery),
        SqlExpr::Const(Value::PgLsn(_)) => SqlType::new(SqlTypeKind::PgLsn),
        SqlExpr::Const(Value::InternalChar(_)) => SqlType::new(SqlTypeKind::InternalChar),
        SqlExpr::Const(Value::EnumOid(_)) => SqlType::new(SqlTypeKind::Enum),
        SqlExpr::Const(Value::Record(record)) => record.sql_type(),
        SqlExpr::Const(Value::Text(_))
        | SqlExpr::Const(Value::TextRef(_, _))
        | SqlExpr::Const(Value::Null) => SqlType::new(SqlTypeKind::Text),
        SqlExpr::Const(Value::Array(_)) => SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
        SqlExpr::Const(Value::PgArray(_)) => SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
        SqlExpr::Const(Value::Float64(_)) => SqlType::new(SqlTypeKind::Float8),
        SqlExpr::ArraySubscript { array, subscripts } => {
            let array_type = infer_sql_expr_type_with_ctes(
                array,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let element_type = array_subscript_element_type(array_type);
            if subscripts.iter().any(|subscript| subscript.upper.is_some()) {
                SqlType::array_of(element_type)
            } else {
                element_type
            }
        }
        SqlExpr::IntegerLiteral(value) => infer_integer_literal_type(value),
        SqlExpr::NumericLiteral(_) => SqlType::new(SqlTypeKind::Numeric),
        SqlExpr::Add(left, right)
        | SqlExpr::BitAnd(left, right)
        | SqlExpr::BitOr(left, right)
        | SqlExpr::BitXor(left, right)
        | SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right) => infer_arithmetic_sql_type(
            expr,
            infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes),
            infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes),
        ),
        SqlExpr::Sub(left, right) => {
            let left_type = infer_sql_expr_type_with_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if left_type == SqlType::new(SqlTypeKind::Jsonb)
                && (!right_type.is_array && is_integer_family(right_type)
                    || (!right_type.is_array && matches!(right_type.kind, SqlTypeKind::Text))
                    || right_type == SqlType::array_of(SqlType::new(SqlTypeKind::Text)))
            {
                SqlType::new(SqlTypeKind::Jsonb)
            } else {
                infer_arithmetic_sql_type(expr, left_type, right_type)
            }
        }
        SqlExpr::Concat(left, right) => infer_concat_sql_type(
            expr,
            infer_sql_expr_type_with_ctes(left, scope, catalog, outer_scopes, grouped_outer, ctes),
            infer_sql_expr_type_with_ctes(right, scope, catalog, outer_scopes, grouped_outer, ctes),
        ),
        SqlExpr::BinaryOperator { op, left, right } => {
            let left_type = infer_sql_expr_type_with_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            match op.as_str() {
                "@@" => SqlType::new(SqlTypeKind::Bool),
                "||" if matches!(left_type.element_type().kind, SqlTypeKind::TsVector)
                    && matches!(right_type.element_type().kind, SqlTypeKind::TsVector) =>
                {
                    SqlType::new(SqlTypeKind::TsVector)
                }
                "&&" | "||"
                    if matches!(left_type.element_type().kind, SqlTypeKind::TsQuery)
                        && matches!(right_type.element_type().kind, SqlTypeKind::TsQuery) =>
                {
                    SqlType::new(SqlTypeKind::TsQuery)
                }
                _ => SqlType::new(SqlTypeKind::Text),
            }
        }
        SqlExpr::Collate { expr: inner, .. } => {
            infer_sql_expr_type_with_ctes(inner, scope, catalog, outer_scopes, grouped_outer, ctes)
        }
        SqlExpr::AtTimeZone { expr, .. } => {
            let source = infer_sql_expr_type_with_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if matches!(source.kind, SqlTypeKind::TimestampTz)
                || matches!(
                    expr.as_ref(),
                    SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
                )
            {
                SqlType::new(SqlTypeKind::Timestamp)
            } else {
                SqlType::new(SqlTypeKind::TimestampTz)
            }
        }
        SqlExpr::UnaryPlus(inner) => {
            infer_sql_expr_type_with_ctes(inner, scope, catalog, outer_scopes, grouped_outer, ctes)
        }
        SqlExpr::Negate(inner) => {
            infer_sql_expr_type_with_ctes(inner, scope, catalog, outer_scopes, grouped_outer, ctes)
        }
        SqlExpr::BitNot(inner) => {
            infer_sql_expr_type_with_ctes(inner, scope, catalog, outer_scopes, grouped_outer, ctes)
        }
        SqlExpr::PrefixOperator { op, expr } => match op.as_str() {
            "!!" => SqlType::new(SqlTypeKind::TsQuery),
            _ => infer_sql_expr_type_with_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        },
        SqlExpr::Cast(_, ty) => {
            resolve_raw_type_name(ty, catalog).unwrap_or_else(|_| raw_type_name_hint(ty))
        }
        SqlExpr::FieldSelect { expr, field } => {
            if let SqlExpr::Column(name) = expr.as_ref()
                && let Some(fields) =
                    resolve_relation_row_expr_with_outer(scope, outer_scopes, name)
                && let Some((_, field_expr)) = fields
                    .iter()
                    .find(|(candidate, _)| candidate.eq_ignore_ascii_case(field))
            {
                expr_sql_type_hint(field_expr).unwrap_or(SqlType::new(SqlTypeKind::Text))
            } else {
                SqlType::new(SqlTypeKind::Text)
            }
        }
        SqlExpr::Eq(_, _)
        | SqlExpr::NotEq(_, _)
        | SqlExpr::Lt(_, _)
        | SqlExpr::LtEq(_, _)
        | SqlExpr::Gt(_, _)
        | SqlExpr::GtEq(_, _)
        | SqlExpr::RegexMatch(_, _)
        | SqlExpr::Like { .. }
        | SqlExpr::Similar { .. }
        | SqlExpr::And(_, _)
        | SqlExpr::Or(_, _)
        | SqlExpr::Not(_)
        | SqlExpr::IsNull(_)
        | SqlExpr::IsNotNull(_)
        | SqlExpr::IsDistinctFrom(_, _)
        | SqlExpr::IsNotDistinctFrom(_, _)
        | SqlExpr::ArrayOverlap(_, _)
        | SqlExpr::ArrayContains(_, _)
        | SqlExpr::ArrayContained(_, _)
        | SqlExpr::JsonbContains(_, _)
        | SqlExpr::JsonbContained(_, _)
        | SqlExpr::JsonbExists(_, _)
        | SqlExpr::JsonbExistsAny(_, _)
        | SqlExpr::JsonbExistsAll(_, _)
        | SqlExpr::JsonbPathExists(_, _)
        | SqlExpr::JsonbPathMatch(_, _)
        | SqlExpr::QuantifiedArray { .. } => SqlType::new(SqlTypeKind::Bool),
        SqlExpr::Case {
            args, defresult, ..
        } => {
            let mut common = defresult.as_deref().map(|expr| {
                infer_sql_expr_type_with_ctes(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
                .element_type()
            });
            for arm in args {
                if matches!(arm.result, SqlExpr::Const(Value::Null)) {
                    continue;
                }
                let ty = infer_sql_expr_type_with_ctes(
                    &arm.result,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
                .element_type();
                common = Some(match common {
                    None => ty,
                    Some(current) => resolve_common_scalar_type(current, ty).unwrap_or(ty),
                });
            }
            common.unwrap_or(SqlType::new(SqlTypeKind::Text))
        }
        SqlExpr::JsonGet(left, _) | SqlExpr::JsonPath(left, _) => {
            let left_type = infer_sql_expr_type_with_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if matches!(left_type.element_type().kind, SqlTypeKind::Jsonb) {
                SqlType::new(SqlTypeKind::Jsonb)
            } else {
                SqlType::new(SqlTypeKind::Json)
            }
        }
        SqlExpr::JsonGetText(_, _) | SqlExpr::JsonPathText(_, _) => SqlType::new(SqlTypeKind::Text),
        SqlExpr::ArrayLiteral(elements) => infer_array_literal_type_with_ctes(
            elements,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )
        .unwrap_or(SqlType::array_of(SqlType::new(SqlTypeKind::Text))),
        SqlExpr::ScalarSubquery(select) => {
            let child_outer = child_outer_scopes(scope, outer_scopes);
            bind_select_query_with_outer(
                select,
                catalog,
                &child_outer,
                grouped_outer.cloned(),
                None,
                ctes,
                &[],
            )
            .ok()
            .and_then(|(plan, _)| {
                let cols = plan.columns();
                if cols.len() == 1 {
                    Some(cols[0].sql_type)
                } else {
                    None
                }
            })
            .unwrap_or(SqlType::new(SqlTypeKind::Text))
        }
        SqlExpr::ArraySubquery(select) => {
            let child_outer = child_outer_scopes(scope, outer_scopes);
            SqlType::array_of(
                bind_select_query_with_outer(
                    select,
                    catalog,
                    &child_outer,
                    grouped_outer.cloned(),
                    None,
                    ctes,
                    &[],
                )
                .ok()
                .and_then(|(plan, _)| {
                    let cols = plan.columns();
                    if cols.len() == 1 {
                        Some(cols[0].sql_type)
                    } else {
                        None
                    }
                })
                .unwrap_or(SqlType::new(SqlTypeKind::Text)),
            )
        }
        SqlExpr::Exists(_) | SqlExpr::InSubquery { .. } | SqlExpr::QuantifiedSubquery { .. } => {
            SqlType::new(SqlTypeKind::Bool)
        }
        SqlExpr::Random => SqlType::new(SqlTypeKind::Float8),
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            within_group,
            distinct,
            func_variadic,
            filter,
            ..
        } => {
            let (direct_args, aggregate_args, aggregate_order_by) =
                normalize_aggregate_call(args, order_by, within_group.as_deref());
            if let Some(result_type) = infer_visible_outer_aggregate_type(
                name,
                &direct_args,
                &aggregate_args,
                &aggregate_order_by,
                *distinct,
                *func_variadic,
                filter.as_deref(),
                catalog,
                outer_scopes,
                ctes,
            ) {
                return result_type;
            }
            let aggregate_arg_types = aggregate_args
                .args()
                .iter()
                .map(|arg| {
                    infer_sql_expr_type_with_ctes(
                        &arg.value,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                })
                .collect::<Vec<_>>();
            if within_group.is_some()
                && let Some(resolved) = resolve_hypothetical_aggregate_call(name)
            {
                return resolved.result_type;
            }
            if let Some(resolved) =
                resolve_aggregate_call(catalog, name, &aggregate_arg_types, *func_variadic)
            {
                return resolved.result_type;
            }
            if name.eq_ignore_ascii_case("coalesce") {
                let values = args
                    .args()
                    .iter()
                    .map(|arg| arg.value.clone())
                    .collect::<Vec<_>>();
                return infer_common_scalar_expr_type_with_ctes(
                    &values,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                    "COALESCE arguments with a common type",
                )
                .unwrap_or(SqlType::new(SqlTypeKind::Text));
            }
            if name.eq_ignore_ascii_case("nullif") {
                let values = args
                    .args()
                    .iter()
                    .map(|arg| arg.value.clone())
                    .collect::<Vec<_>>();
                return infer_common_scalar_expr_type_with_ctes(
                    &values,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                    "NULLIF arguments with a common type",
                )
                .unwrap_or(SqlType::new(SqlTypeKind::Text));
            }
            if name.eq_ignore_ascii_case("xmlconcat") {
                return SqlType::new(SqlTypeKind::Xml);
            }
            let actual_types = args
                .args()
                .iter()
                .map(|arg| {
                    infer_sql_expr_type_with_ctes(
                        &arg.value,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                })
                .collect::<Vec<_>>();
            if matches!(args.args().len(), 3)
                && !*func_variadic
                && (name.eq_ignore_ascii_case("lag") || name.eq_ignore_ascii_case("lead"))
                && let Ok(common_type) = infer_common_scalar_expr_type_with_ctes(
                    &[args.args()[0].value.clone(), args.args()[2].value.clone()],
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                    "lag/lead value and default arguments with a common type",
                )
            {
                let mut resolution_types = actual_types.clone();
                resolution_types[0] = common_type;
                resolution_types[2] = common_type;
                if let Ok(resolved) =
                    resolve_function_call(catalog, name, &resolution_types, *func_variadic)
                {
                    return resolved.result_type;
                }
            }
            if let Ok(resolved) =
                resolve_function_call(catalog, name, &actual_types, *func_variadic)
            {
                return resolved.result_type;
            }
            let resolved = resolve_scalar_function(name);
            if let Some(BuiltinScalarFunction::RangeConstructor) = resolved
                && let Some(target_type) = resolve_function_cast_type(catalog, name)
                && range_type_ref_for_sql_type(target_type).is_some()
            {
                return target_type;
            }
            if let Some(func) = resolved
                && let Some(sql_type) = fixed_scalar_return_type(func)
            {
                return sql_type;
            }
            if let Some(func) = resolved
                && let Some(sql_type) = infer_geometry_function_return_type_with_ctes(
                    func,
                    args,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            {
                return sql_type;
            }
            match resolved {
                Some(BuiltinScalarFunction::TsMatch) => SqlType::new(SqlTypeKind::Bool),
                Some(BuiltinScalarFunction::ToTsVector) => SqlType::new(SqlTypeKind::TsVector),
                Some(
                    BuiltinScalarFunction::ToTsQuery
                    | BuiltinScalarFunction::PlainToTsQuery
                    | BuiltinScalarFunction::PhraseToTsQuery
                    | BuiltinScalarFunction::WebSearchToTsQuery,
                ) => SqlType::new(SqlTypeKind::TsQuery),
                Some(BuiltinScalarFunction::TsLexize) => {
                    SqlType::array_of(SqlType::new(SqlTypeKind::Text))
                }
                Some(
                    BuiltinScalarFunction::TsQueryAnd
                    | BuiltinScalarFunction::TsQueryOr
                    | BuiltinScalarFunction::TsQueryNot,
                ) => SqlType::new(SqlTypeKind::TsQuery),
                Some(BuiltinScalarFunction::TsVectorConcat) => SqlType::new(SqlTypeKind::TsVector),
                Some(BuiltinScalarFunction::Random) => SqlType::new(SqlTypeKind::Float8),
                Some(BuiltinScalarFunction::CashLarger | BuiltinScalarFunction::CashSmaller) => {
                    SqlType::new(SqlTypeKind::Money)
                }
                Some(BuiltinScalarFunction::CashWords) => SqlType::new(SqlTypeKind::Text),
                Some(BuiltinScalarFunction::NetworkHost | BuiltinScalarFunction::NetworkAbbrev) => {
                    SqlType::new(SqlTypeKind::Text)
                }
                Some(
                    BuiltinScalarFunction::NetworkMasklen | BuiltinScalarFunction::NetworkFamily,
                ) => SqlType::new(SqlTypeKind::Int4),
                Some(
                    BuiltinScalarFunction::NetworkSameFamily
                    | BuiltinScalarFunction::NetworkSubnet
                    | BuiltinScalarFunction::NetworkSubnetEq
                    | BuiltinScalarFunction::NetworkSupernet
                    | BuiltinScalarFunction::NetworkSupernetEq
                    | BuiltinScalarFunction::NetworkOverlap,
                ) => SqlType::new(SqlTypeKind::Bool),
                Some(
                    BuiltinScalarFunction::NetworkBroadcast
                    | BuiltinScalarFunction::NetworkNetmask
                    | BuiltinScalarFunction::NetworkHostmask,
                ) => SqlType::new(SqlTypeKind::Inet),
                Some(
                    BuiltinScalarFunction::NetworkNetwork | BuiltinScalarFunction::NetworkMerge,
                ) => SqlType::new(SqlTypeKind::Cidr),
                Some(BuiltinScalarFunction::NetworkSetMasklen) => args
                    .args()
                    .first()
                    .map(|arg| {
                        infer_sql_expr_type_with_ctes(
                            &arg.value,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )
                    })
                    .unwrap_or(SqlType::new(SqlTypeKind::Inet)),
                Some(BuiltinScalarFunction::ArrayNdims)
                | Some(BuiltinScalarFunction::ArrayLower) => SqlType::new(SqlTypeKind::Int4),
                Some(BuiltinScalarFunction::ArrayDims) => SqlType::new(SqlTypeKind::Text),
                Some(BuiltinScalarFunction::Now)
                | Some(BuiltinScalarFunction::TransactionTimestamp)
                | Some(BuiltinScalarFunction::StatementTimestamp)
                | Some(BuiltinScalarFunction::ClockTimestamp) => {
                    SqlType::new(SqlTypeKind::TimestampTz)
                }
                Some(BuiltinScalarFunction::TimeOfDay) => SqlType::new(SqlTypeKind::Text),
                Some(BuiltinScalarFunction::ToRegProc) => SqlType::new(SqlTypeKind::RegProc),
                Some(BuiltinScalarFunction::ToRegProcedure) => {
                    SqlType::new(SqlTypeKind::RegProcedure)
                }
                Some(BuiltinScalarFunction::ToRegOper) => SqlType::new(SqlTypeKind::RegOper),
                Some(BuiltinScalarFunction::ToRegOperator) => {
                    SqlType::new(SqlTypeKind::RegOperator)
                }
                Some(BuiltinScalarFunction::ToRegClass) => SqlType::new(SqlTypeKind::RegClass),
                Some(BuiltinScalarFunction::ToRegType) => SqlType::new(SqlTypeKind::RegType),
                Some(BuiltinScalarFunction::ToRegTypeMod) => SqlType::new(SqlTypeKind::Int4),
                Some(BuiltinScalarFunction::ToRegRole) => SqlType::new(SqlTypeKind::RegRole),
                Some(BuiltinScalarFunction::ToRegNamespace) => {
                    SqlType::new(SqlTypeKind::RegNamespace)
                }
                Some(BuiltinScalarFunction::ToRegCollation) => {
                    SqlType::new(SqlTypeKind::RegCollation)
                }
                Some(BuiltinScalarFunction::Timezone) => {
                    let source_index = if args.args().len() == 1 { 0 } else { 1 };
                    match args.args().get(source_index).map(|arg| {
                        infer_sql_expr_type_with_ctes(
                            &arg.value,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )
                    }) {
                        Some(SqlType {
                            kind: SqlTypeKind::TimestampTz,
                            ..
                        }) => SqlType::new(SqlTypeKind::Timestamp),
                        Some(SqlType {
                            kind: SqlTypeKind::TimeTz,
                            ..
                        }) => SqlType::new(SqlTypeKind::TimeTz),
                        Some(_) => SqlType::new(SqlTypeKind::TimestampTz),
                        None => SqlType::new(SqlTypeKind::TimestampTz),
                    }
                }
                Some(BuiltinScalarFunction::DatePart) => SqlType::new(SqlTypeKind::Float8),
                Some(BuiltinScalarFunction::Extract) => SqlType::new(SqlTypeKind::Numeric),
                Some(BuiltinScalarFunction::TimeZone) => SqlType::new(SqlTypeKind::TimeTz),
                Some(BuiltinScalarFunction::DateTrunc) => match args.args().get(1).map(|arg| {
                    infer_sql_expr_type_with_ctes(
                        &arg.value,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                }) {
                    Some(SqlType {
                        kind: SqlTypeKind::Date,
                        ..
                    }) => SqlType::new(SqlTypeKind::TimestampTz),
                    Some(SqlType {
                        kind: SqlTypeKind::Timestamp,
                        ..
                    }) => SqlType::new(SqlTypeKind::Timestamp),
                    Some(SqlType {
                        kind: SqlTypeKind::TimestampTz,
                        ..
                    }) => SqlType::new(SqlTypeKind::TimestampTz),
                    _ => SqlType::new(SqlTypeKind::Timestamp),
                },
                Some(BuiltinScalarFunction::DateBin) => match args.args().get(1).map(|arg| {
                    infer_sql_expr_type_with_ctes(
                        &arg.value,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                }) {
                    Some(SqlType {
                        kind: SqlTypeKind::TimestampTz,
                        ..
                    }) => SqlType::new(SqlTypeKind::TimestampTz),
                    _ => SqlType::new(SqlTypeKind::Timestamp),
                },
                Some(BuiltinScalarFunction::DateAdd)
                | Some(BuiltinScalarFunction::DateSubtract)
                | Some(BuiltinScalarFunction::ToTimestamp) => {
                    SqlType::new(SqlTypeKind::TimestampTz)
                }
                Some(BuiltinScalarFunction::IsFinite) => SqlType::new(SqlTypeKind::Bool),
                Some(
                    BuiltinScalarFunction::MacAddrEq
                    | BuiltinScalarFunction::MacAddrNe
                    | BuiltinScalarFunction::MacAddrLt
                    | BuiltinScalarFunction::MacAddrLe
                    | BuiltinScalarFunction::MacAddrGt
                    | BuiltinScalarFunction::MacAddrGe
                    | BuiltinScalarFunction::MacAddr8Eq
                    | BuiltinScalarFunction::MacAddr8Ne
                    | BuiltinScalarFunction::MacAddr8Lt
                    | BuiltinScalarFunction::MacAddr8Le
                    | BuiltinScalarFunction::MacAddr8Gt
                    | BuiltinScalarFunction::MacAddr8Ge,
                ) => SqlType::new(SqlTypeKind::Bool),
                Some(
                    BuiltinScalarFunction::MacAddrCmp
                    | BuiltinScalarFunction::MacAddr8Cmp
                    | BuiltinScalarFunction::HashMacAddr
                    | BuiltinScalarFunction::HashMacAddr8,
                ) => SqlType::new(SqlTypeKind::Int4),
                Some(
                    BuiltinScalarFunction::HashMacAddrExtended
                    | BuiltinScalarFunction::HashMacAddr8Extended,
                ) => SqlType::new(SqlTypeKind::Int8),
                Some(
                    BuiltinScalarFunction::MacAddrNot
                    | BuiltinScalarFunction::MacAddrAnd
                    | BuiltinScalarFunction::MacAddrOr
                    | BuiltinScalarFunction::MacAddrTrunc
                    | BuiltinScalarFunction::MacAddr8ToMacAddr,
                ) => SqlType::new(SqlTypeKind::MacAddr),
                Some(
                    BuiltinScalarFunction::MacAddrToMacAddr8
                    | BuiltinScalarFunction::MacAddr8Not
                    | BuiltinScalarFunction::MacAddr8And
                    | BuiltinScalarFunction::MacAddr8Or
                    | BuiltinScalarFunction::MacAddr8Trunc
                    | BuiltinScalarFunction::MacAddr8Set7Bit,
                ) => SqlType::new(SqlTypeKind::MacAddr8),
                Some(BuiltinScalarFunction::MakeDate) => SqlType::new(SqlTypeKind::Date),
                Some(BuiltinScalarFunction::MakeTime) => SqlType::new(SqlTypeKind::Time),
                Some(BuiltinScalarFunction::MakeTimestamp) => SqlType::new(SqlTypeKind::Timestamp),
                Some(BuiltinScalarFunction::MakeTimestampTz) => {
                    SqlType::new(SqlTypeKind::TimestampTz)
                }
                Some(BuiltinScalarFunction::Age) => SqlType::new(SqlTypeKind::Interval),
                Some(BuiltinScalarFunction::ToJson)
                | Some(BuiltinScalarFunction::ArrayToJson)
                | Some(BuiltinScalarFunction::JsonBuildArray)
                | Some(BuiltinScalarFunction::JsonBuildObject)
                | Some(BuiltinScalarFunction::JsonObject)
                | Some(BuiltinScalarFunction::JsonStripNulls) => SqlType::new(SqlTypeKind::Json),
                Some(BuiltinScalarFunction::ToJsonb)
                | Some(BuiltinScalarFunction::JsonbObject)
                | Some(BuiltinScalarFunction::JsonbExtractPath)
                | Some(BuiltinScalarFunction::JsonbStripNulls)
                | Some(BuiltinScalarFunction::JsonbBuildArray)
                | Some(BuiltinScalarFunction::JsonbBuildObject)
                | Some(BuiltinScalarFunction::JsonbDelete)
                | Some(BuiltinScalarFunction::JsonbDeletePath)
                | Some(BuiltinScalarFunction::JsonbSet)
                | Some(BuiltinScalarFunction::JsonbSetLax)
                | Some(BuiltinScalarFunction::JsonbInsert)
                | Some(BuiltinScalarFunction::JsonbPathQueryArray)
                | Some(BuiltinScalarFunction::JsonbPathQueryFirst) => {
                    SqlType::new(SqlTypeKind::Jsonb)
                }
                Some(BuiltinScalarFunction::GetDatabaseEncoding)
                | Some(BuiltinScalarFunction::Initcap)
                | Some(BuiltinScalarFunction::JsonTypeof)
                | Some(BuiltinScalarFunction::JsonExtractPathText)
                | Some(BuiltinScalarFunction::JsonbPretty)
                | Some(BuiltinScalarFunction::JsonbTypeof)
                | Some(BuiltinScalarFunction::JsonbExtractPathText)
                | Some(BuiltinScalarFunction::BpcharToText)
                | Some(BuiltinScalarFunction::Concat)
                | Some(BuiltinScalarFunction::ConcatWs)
                | Some(BuiltinScalarFunction::Format)
                | Some(BuiltinScalarFunction::Lower)
                | Some(BuiltinScalarFunction::Unistr)
                | Some(BuiltinScalarFunction::BTrim)
                | Some(BuiltinScalarFunction::LTrim)
                | Some(BuiltinScalarFunction::RTrim)
                | Some(BuiltinScalarFunction::Left)
                | Some(BuiltinScalarFunction::Right)
                | Some(BuiltinScalarFunction::LPad)
                | Some(BuiltinScalarFunction::RPad)
                | Some(BuiltinScalarFunction::Repeat)
                | Some(BuiltinScalarFunction::Md5)
                | Some(BuiltinScalarFunction::Chr)
                | Some(BuiltinScalarFunction::QuoteLiteral)
                | Some(BuiltinScalarFunction::FormatType)
                | Some(BuiltinScalarFunction::RegProcToText)
                | Some(BuiltinScalarFunction::RegOperToText)
                | Some(BuiltinScalarFunction::RegOperatorToText)
                | Some(BuiltinScalarFunction::RegProcedureToText)
                | Some(BuiltinScalarFunction::RegCollationToText)
                | Some(BuiltinScalarFunction::RegClassToText)
                | Some(BuiltinScalarFunction::RegTypeToText)
                | Some(BuiltinScalarFunction::RegRoleToText)
                | Some(BuiltinScalarFunction::Replace)
                | Some(BuiltinScalarFunction::SplitPart)
                | Some(BuiltinScalarFunction::Translate)
                | Some(BuiltinScalarFunction::ConvertFrom)
                | Some(BuiltinScalarFunction::Encode)
                | Some(BuiltinScalarFunction::RegexpSubstr)
                | Some(BuiltinScalarFunction::RegexpReplace)
                | Some(BuiltinScalarFunction::SimilarSubstring)
                | Some(BuiltinScalarFunction::ToBin)
                | Some(BuiltinScalarFunction::ToOct)
                | Some(BuiltinScalarFunction::ToHex) => SqlType::new(SqlTypeKind::Text),
                Some(BuiltinScalarFunction::PgLsn) => SqlType::new(SqlTypeKind::PgLsn),
                Some(BuiltinScalarFunction::Decode)
                | Some(BuiltinScalarFunction::Sha224)
                | Some(BuiltinScalarFunction::Sha256)
                | Some(BuiltinScalarFunction::Sha384)
                | Some(BuiltinScalarFunction::Sha512) => SqlType::new(SqlTypeKind::Bytea),
                Some(BuiltinScalarFunction::Length)
                | Some(BuiltinScalarFunction::ArrayLength)
                | Some(BuiltinScalarFunction::Cardinality)
                | Some(BuiltinScalarFunction::ArrayPosition)
                | Some(BuiltinScalarFunction::Ascii)
                | Some(BuiltinScalarFunction::RegexpCount)
                | Some(BuiltinScalarFunction::RegexpInstr)
                | Some(BuiltinScalarFunction::JsonArrayLength)
                | Some(BuiltinScalarFunction::JsonbArrayLength)
                | Some(BuiltinScalarFunction::PgColumnSize)
                | Some(BuiltinScalarFunction::Scale)
                | Some(BuiltinScalarFunction::MinScale)
                | Some(BuiltinScalarFunction::WidthBucket)
                | Some(BuiltinScalarFunction::GetByte) => SqlType::new(SqlTypeKind::Int4),
                Some(BuiltinScalarFunction::PgRelationSize) => SqlType::new(SqlTypeKind::Int8),
                Some(BuiltinScalarFunction::Crc32)
                | Some(BuiltinScalarFunction::Crc32c)
                | Some(BuiltinScalarFunction::BitCount) => SqlType::new(SqlTypeKind::Int8),
                Some(
                    BuiltinScalarFunction::RegexpMatch | BuiltinScalarFunction::RegexpSplitToArray,
                ) => SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                Some(BuiltinScalarFunction::StringToArray) => {
                    SqlType::array_of(SqlType::new(SqlTypeKind::Text))
                }
                Some(BuiltinScalarFunction::ArrayPositions) => {
                    SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
                }
                Some(
                    BuiltinScalarFunction::ArrayAppend
                    | BuiltinScalarFunction::ArrayPrepend
                    | BuiltinScalarFunction::ArrayCat
                    | BuiltinScalarFunction::ArrayRemove
                    | BuiltinScalarFunction::ArrayReplace
                    | BuiltinScalarFunction::ArraySort,
                ) => function_arg_values(args).next().map_or(
                    SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                    |arg| {
                        infer_sql_expr_type_with_ctes(
                            arg,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )
                    },
                ),
                Some(BuiltinScalarFunction::Position | BuiltinScalarFunction::Strpos) => {
                    SqlType::new(SqlTypeKind::Int4)
                }
                Some(BuiltinScalarFunction::ArrayToString) => SqlType::new(SqlTypeKind::Text),
                Some(BuiltinScalarFunction::ArrayFill) => function_arg_values(args).next().map_or(
                    SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                    |arg| {
                        SqlType::array_of(
                            infer_sql_expr_type_with_ctes(
                                arg,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            )
                            .element_type(),
                        )
                    },
                ),
                Some(BuiltinScalarFunction::Substring | BuiltinScalarFunction::Overlay) => {
                    function_arg_values(args).next().map_or(
                        SqlType::new(SqlTypeKind::Text),
                        |arg| {
                            infer_sql_expr_type_with_ctes(
                                arg,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            )
                        },
                    )
                }
                Some(BuiltinScalarFunction::Reverse) => function_arg_values(args).next().map_or(
                    SqlType::new(SqlTypeKind::Text),
                    |arg| {
                        infer_sql_expr_type_with_ctes(
                            arg,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )
                    },
                ),
                Some(BuiltinScalarFunction::GetBit) => SqlType::new(SqlTypeKind::Int4),
                Some(BuiltinScalarFunction::SetBit | BuiltinScalarFunction::SetByte) => {
                    function_arg_values(args).next().map_or(
                        SqlType::new(SqlTypeKind::Text),
                        |arg| {
                            infer_sql_expr_type_with_ctes(
                                arg,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            )
                        },
                    )
                }
                Some(BuiltinScalarFunction::JsonbPathExists)
                | Some(BuiltinScalarFunction::JsonbPathMatch)
                | Some(BuiltinScalarFunction::RegexpLike) => SqlType::new(SqlTypeKind::Bool),
                Some(BuiltinScalarFunction::JsonExtractPath) => SqlType::new(SqlTypeKind::Json),
                Some(BuiltinScalarFunction::Abs) => function_arg_values(args).next().map_or(
                    SqlType::new(SqlTypeKind::Text),
                    |arg| {
                        infer_sql_expr_type_with_ctes(
                            arg,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )
                    },
                ),
                Some(BuiltinScalarFunction::Div)
                | Some(BuiltinScalarFunction::Mod)
                | Some(BuiltinScalarFunction::TrimScale)
                | Some(BuiltinScalarFunction::NumericInc)
                | Some(BuiltinScalarFunction::Factorial) => SqlType::new(SqlTypeKind::Numeric),
                Some(
                    BuiltinScalarFunction::Cbrt
                    | BuiltinScalarFunction::Sinh
                    | BuiltinScalarFunction::Cosh
                    | BuiltinScalarFunction::Tanh
                    | BuiltinScalarFunction::Asinh
                    | BuiltinScalarFunction::Acosh
                    | BuiltinScalarFunction::Atanh
                    | BuiltinScalarFunction::Sind
                    | BuiltinScalarFunction::Cosd
                    | BuiltinScalarFunction::Tand
                    | BuiltinScalarFunction::Cotd
                    | BuiltinScalarFunction::Asind
                    | BuiltinScalarFunction::Acosd
                    | BuiltinScalarFunction::Atand
                    | BuiltinScalarFunction::Atan2d
                    | BuiltinScalarFunction::Erf
                    | BuiltinScalarFunction::Erfc
                    | BuiltinScalarFunction::Gamma
                    | BuiltinScalarFunction::Lgamma,
                ) => SqlType::new(SqlTypeKind::Float8),
                Some(
                    BuiltinScalarFunction::Sqrt
                    | BuiltinScalarFunction::Exp
                    | BuiltinScalarFunction::Ln,
                ) => args
                    .args()
                    .first()
                    .map_or(SqlType::new(SqlTypeKind::Float8), |arg| {
                        let ty = infer_sql_expr_type_with_ctes(
                            &arg.value,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        );
                        match ty.element_type().kind {
                            SqlTypeKind::Float4 | SqlTypeKind::Float8 => {
                                SqlType::new(SqlTypeKind::Float8)
                            }
                            _ if is_numeric_family(ty) => SqlType::new(SqlTypeKind::Numeric),
                            _ => SqlType::new(SqlTypeKind::Float8),
                        }
                    }),
                Some(BuiltinScalarFunction::Power) => {
                    let left = args.args().first().map(|arg| {
                        infer_sql_expr_type_with_ctes(
                            &arg.value,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )
                    });
                    let right = args.args().get(1).map(|arg| {
                        infer_sql_expr_type_with_ctes(
                            &arg.value,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )
                    });
                    match (left, right) {
                        (Some(left), Some(right))
                            if matches!(
                                left.element_type().kind,
                                SqlTypeKind::Float4 | SqlTypeKind::Float8
                            ) || matches!(
                                right.element_type().kind,
                                SqlTypeKind::Float4 | SqlTypeKind::Float8
                            ) =>
                        {
                            SqlType::new(SqlTypeKind::Float8)
                        }
                        (Some(left), Some(right))
                            if is_numeric_family(left) && is_numeric_family(right) =>
                        {
                            SqlType::new(SqlTypeKind::Numeric)
                        }
                        _ => SqlType::new(SqlTypeKind::Float8),
                    }
                }
                Some(BuiltinScalarFunction::Log | BuiltinScalarFunction::Log10) => {
                    if args.args().len() == 2 {
                        function_arg_values(args).next().map_or(
                            SqlType::new(SqlTypeKind::Float8),
                            |arg| {
                                let ty = infer_sql_expr_type_with_ctes(
                                    arg,
                                    scope,
                                    catalog,
                                    outer_scopes,
                                    grouped_outer,
                                    ctes,
                                );
                                match ty.element_type().kind {
                                    SqlTypeKind::Float4 | SqlTypeKind::Float8 => {
                                        SqlType::new(SqlTypeKind::Float8)
                                    }
                                    _ if is_numeric_family(ty) => {
                                        SqlType::new(SqlTypeKind::Numeric)
                                    }
                                    _ => SqlType::new(SqlTypeKind::Float8),
                                }
                            },
                        )
                    } else {
                        function_arg_values(args).next().map_or(
                            SqlType::new(SqlTypeKind::Float8),
                            |arg| {
                                let ty = infer_sql_expr_type_with_ctes(
                                    arg,
                                    scope,
                                    catalog,
                                    outer_scopes,
                                    grouped_outer,
                                    ctes,
                                );
                                match ty.element_type().kind {
                                    SqlTypeKind::Float4 | SqlTypeKind::Float8 => {
                                        SqlType::new(SqlTypeKind::Float8)
                                    }
                                    _ if is_numeric_family(ty) => {
                                        SqlType::new(SqlTypeKind::Numeric)
                                    }
                                    _ => SqlType::new(SqlTypeKind::Float8),
                                }
                            },
                        )
                    }
                }
                Some(
                    BuiltinScalarFunction::Ceil
                    | BuiltinScalarFunction::Ceiling
                    | BuiltinScalarFunction::Floor
                    | BuiltinScalarFunction::Sign,
                ) => function_arg_values(args).next().map_or(
                    SqlType::new(SqlTypeKind::Float8),
                    |arg| {
                        let ty = infer_sql_expr_type_with_ctes(
                            arg,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        );
                        match ty.element_type().kind {
                            SqlTypeKind::Float4 | SqlTypeKind::Float8 => {
                                SqlType::new(SqlTypeKind::Float8)
                            }
                            _ if is_numeric_family(ty) => SqlType::new(SqlTypeKind::Numeric),
                            _ => SqlType::new(SqlTypeKind::Float8),
                        }
                    },
                ),
                Some(BuiltinScalarFunction::Trunc | BuiltinScalarFunction::Round) => {
                    function_arg_values(args).next().map_or(
                        SqlType::new(SqlTypeKind::Float8),
                        |arg| {
                            let ty = infer_sql_expr_type_with_ctes(
                                arg,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            );
                            match ty.element_type().kind {
                                SqlTypeKind::Float4 | SqlTypeKind::Float8 => {
                                    SqlType::new(SqlTypeKind::Float8)
                                }
                                _ if is_numeric_family(ty) => SqlType::new(SqlTypeKind::Numeric),
                                _ => SqlType::new(SqlTypeKind::Float8),
                            }
                        },
                    )
                }
                Some(BuiltinScalarFunction::BitcastIntegerToFloat4) => {
                    SqlType::new(SqlTypeKind::Float4)
                }
                Some(BuiltinScalarFunction::BitcastBigintToFloat8) => {
                    SqlType::new(SqlTypeKind::Float8)
                }
                Some(BuiltinScalarFunction::Float4Send | BuiltinScalarFunction::Float8Send) => {
                    SqlType::new(SqlTypeKind::Text)
                }
                Some(BuiltinScalarFunction::BoolEq | BuiltinScalarFunction::BoolNe) => {
                    SqlType::new(SqlTypeKind::Bool)
                }
                Some(BuiltinScalarFunction::Gcd) | Some(BuiltinScalarFunction::Lcm) => {
                    function_arg_values(args)
                        .next()
                        .zip(function_arg_values(args).nth(1))
                        .map_or(SqlType::new(SqlTypeKind::Text), |(left, right)| {
                            resolve_numeric_binary_type(
                                "+",
                                infer_sql_expr_type_with_ctes(
                                    left,
                                    scope,
                                    catalog,
                                    outer_scopes,
                                    grouped_outer,
                                    ctes,
                                ),
                                infer_sql_expr_type_with_ctes(
                                    right,
                                    scope,
                                    catalog,
                                    outer_scopes,
                                    grouped_outer,
                                    ctes,
                                ),
                            )
                            .unwrap_or(SqlType::new(SqlTypeKind::Text))
                        })
                }
                Some(BuiltinScalarFunction::PgInputIsValid) => SqlType::new(SqlTypeKind::Bool),
                Some(BuiltinScalarFunction::TxidVisibleInSnapshot) => {
                    SqlType::new(SqlTypeKind::Bool)
                }
                Some(BuiltinScalarFunction::TxidCurrent)
                | Some(BuiltinScalarFunction::TxidCurrentIfAssigned) => {
                    SqlType::new(SqlTypeKind::Int8)
                }
                Some(BuiltinScalarFunction::ToDate) => SqlType::new(SqlTypeKind::Date),
                Some(BuiltinScalarFunction::ToNumber) => SqlType::new(SqlTypeKind::Numeric),
                Some(BuiltinScalarFunction::ToChar)
                | Some(BuiltinScalarFunction::PgInputErrorMessage)
                | Some(BuiltinScalarFunction::PgInputErrorDetail)
                | Some(BuiltinScalarFunction::PgInputErrorHint)
                | Some(BuiltinScalarFunction::PgInputErrorSqlState) => {
                    SqlType::new(SqlTypeKind::Text)
                }
                None => resolve_function_cast_type(catalog, name)
                    .unwrap_or(SqlType::new(SqlTypeKind::Text)),
                Some(_) => SqlType::new(SqlTypeKind::Text),
            }
        }
        SqlExpr::Subscript { .. }
        | SqlExpr::GeometryUnaryOp { .. }
        | SqlExpr::GeometryBinaryOp { .. } => unreachable!("handled before match"),
        SqlExpr::CurrentDate => SqlType::new(SqlTypeKind::Date),
        SqlExpr::CurrentCatalog => SqlType::new(SqlTypeKind::Text),
        SqlExpr::CurrentSchema => SqlType::new(SqlTypeKind::Text),
        SqlExpr::CurrentUser => SqlType::new(SqlTypeKind::Name),
        SqlExpr::SessionUser => SqlType::new(SqlTypeKind::Name),
        SqlExpr::CurrentRole => SqlType::new(SqlTypeKind::Name),
        SqlExpr::CurrentTime { precision } => precision
            .map(|precision| SqlType::with_time_precision(SqlTypeKind::TimeTz, precision))
            .unwrap_or_else(|| SqlType::new(SqlTypeKind::TimeTz)),
        SqlExpr::CurrentTimestamp { precision } => precision
            .map(|precision| SqlType::with_time_precision(SqlTypeKind::TimestampTz, precision))
            .unwrap_or_else(|| SqlType::new(SqlTypeKind::TimestampTz)),
        SqlExpr::LocalTime { precision } => precision
            .map(|precision| SqlType::with_time_precision(SqlTypeKind::Time, precision))
            .unwrap_or_else(|| SqlType::new(SqlTypeKind::Time)),
        SqlExpr::LocalTimestamp { precision } => precision
            .map(|precision| SqlType::with_time_precision(SqlTypeKind::Timestamp, precision))
            .unwrap_or_else(|| SqlType::new(SqlTypeKind::Timestamp)),
        SqlExpr::Xml(_) => SqlType::new(SqlTypeKind::Xml),
    }
}

fn infer_sql_row_expr_type(
    items: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> SqlType {
    let mut fields = Vec::new();
    let mut next_index = 1usize;
    for item in items {
        if let SqlExpr::Column(name) = item
            && let Some(relation_name) = name.strip_suffix(".*")
        {
            if let Some(relation_fields) =
                resolve_relation_row_expr_with_outer(scope, outer_scopes, relation_name)
            {
                for (_, expr) in relation_fields {
                    fields.push((
                        format!("f{next_index}"),
                        expr_sql_type_hint(&expr).unwrap_or(SqlType::new(SqlTypeKind::Text)),
                    ));
                    next_index += 1;
                }
            } else {
                fields.push((format!("f{next_index}"), SqlType::new(SqlTypeKind::Text)));
                next_index += 1;
            }
            continue;
        }

        fields.push((
            format!("f{next_index}"),
            infer_sql_expr_type_with_ctes(item, scope, catalog, outer_scopes, grouped_outer, ctes),
        ));
        next_index += 1;
    }

    assign_anonymous_record_descriptor(fields).sql_type()
}

pub(super) fn infer_common_scalar_expr_type_with_ctes(
    exprs: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
    expected: &'static str,
) -> Result<SqlType, ParseError> {
    let mut common: Option<SqlType> = None;
    for expr in exprs {
        if matches!(expr, SqlExpr::Const(Value::Null)) {
            continue;
        }
        let ty =
            infer_sql_expr_type_with_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)
                .element_type();
        common = Some(match common {
            None => ty,
            Some(current) => resolve_common_scalar_type(current, ty).ok_or_else(|| {
                ParseError::UnexpectedToken {
                    expected,
                    actual: format!("{} and {}", sql_type_name(current), sql_type_name(ty)),
                }
            })?,
        });
    }
    Ok(common.unwrap_or(SqlType::new(SqlTypeKind::Text)))
}

pub(super) fn infer_array_literal_type_with_ctes(
    elements: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<SqlType> {
    let mut common = None;
    for element in elements {
        if matches!(element, SqlExpr::Const(Value::Null)) {
            continue;
        }
        let ty = infer_sql_expr_type_with_ctes(
            element,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
        common = Some(match common {
            None => ty.element_type(),
            Some(existing) => resolve_common_scalar_type(existing, ty)?,
        });
    }
    common.map(SqlType::array_of)
}

pub(super) fn infer_array_literal_type(
    elements: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<SqlType, ParseError> {
    let mut common: Option<SqlType> = None;
    for element in elements {
        if matches!(element, SqlExpr::Const(Value::Null)) {
            continue;
        }
        let ty = infer_sql_expr_type(element, scope, catalog, outer_scopes, grouped_outer)
            .element_type();
        common = Some(match common {
            None => ty,
            Some(current) => resolve_common_scalar_type(current, ty).ok_or_else(|| {
                ParseError::UnexpectedToken {
                    expected: "array literal elements with a common type",
                    actual: format!("{} and {}", sql_type_name(current), sql_type_name(ty)),
                }
            })?,
        });
    }
    let Some(common) = common else {
        return Err(ParseError::UnexpectedToken {
            expected: "ARRAY[...] with a typed element or explicit cast",
            actual: "ARRAY[]".into(),
        });
    };
    Ok(SqlType::array_of(common))
}
