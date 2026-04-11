use super::*;
use super::expr::{bind_concat_operands, resolve_scalar_function, validate_scalar_function_arity};

pub(super) fn infer_sql_expr_type(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> SqlType {
    match expr {
        SqlExpr::Column(name) => {
            match resolve_column_with_outer(scope, outer_scopes, name, grouped_outer) {
                Ok(ResolvedColumn::Local(idx)) => scope.desc.columns.get(idx).map(|c| c.sql_type),
                Ok(ResolvedColumn::Outer { depth, index }) => outer_scopes
                    .get(depth)
                    .and_then(|s| s.desc.columns.get(index).map(|c| c.sql_type)),
                Err(_) => None,
            }
            .unwrap_or(SqlType::new(SqlTypeKind::Text))
        }
        SqlExpr::Const(Value::Int16(_)) => SqlType::new(SqlTypeKind::Int2),
        SqlExpr::Const(Value::Int32(_)) => SqlType::new(SqlTypeKind::Int4),
        SqlExpr::Const(Value::Int64(_)) => SqlType::new(SqlTypeKind::Int8),
        SqlExpr::Const(Value::Bool(_)) => SqlType::new(SqlTypeKind::Bool),
        SqlExpr::Const(Value::Numeric(_)) => SqlType::new(SqlTypeKind::Numeric),
        SqlExpr::Const(Value::Json(_)) => SqlType::new(SqlTypeKind::Json),
        SqlExpr::Const(Value::Jsonb(_)) => SqlType::new(SqlTypeKind::Jsonb),
        SqlExpr::Const(Value::JsonPath(_)) => SqlType::new(SqlTypeKind::JsonPath),
        SqlExpr::Const(Value::Text(_))
        | SqlExpr::Const(Value::TextRef(_, _))
        | SqlExpr::Const(Value::Null) => SqlType::new(SqlTypeKind::Text),
        SqlExpr::Const(Value::Array(_)) => SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
        SqlExpr::Const(Value::Float64(_)) => SqlType::new(SqlTypeKind::Float8),
        SqlExpr::IntegerLiteral(value) => infer_integer_literal_type(value),
        SqlExpr::NumericLiteral(_) => SqlType::new(SqlTypeKind::Numeric),
        SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right) => infer_arithmetic_sql_type(
            expr,
            infer_sql_expr_type(left, scope, catalog, outer_scopes, grouped_outer),
            infer_sql_expr_type(right, scope, catalog, outer_scopes, grouped_outer),
        ),
        SqlExpr::Concat(left, right) => infer_concat_sql_type(
            expr,
            infer_sql_expr_type(left, scope, catalog, outer_scopes, grouped_outer),
            infer_sql_expr_type(right, scope, catalog, outer_scopes, grouped_outer),
        ),
        SqlExpr::UnaryPlus(inner) => {
            infer_sql_expr_type(inner, scope, catalog, outer_scopes, grouped_outer)
        }
        SqlExpr::Negate(inner) => {
            infer_sql_expr_type(inner, scope, catalog, outer_scopes, grouped_outer)
        }
        SqlExpr::Cast(_, ty) => *ty,
        SqlExpr::Eq(_, _)
        | SqlExpr::NotEq(_, _)
        | SqlExpr::Lt(_, _)
        | SqlExpr::LtEq(_, _)
        | SqlExpr::Gt(_, _)
        | SqlExpr::GtEq(_, _)
        | SqlExpr::RegexMatch(_, _)
        | SqlExpr::And(_, _)
        | SqlExpr::Or(_, _)
        | SqlExpr::Not(_)
        | SqlExpr::IsNull(_)
        | SqlExpr::IsNotNull(_)
        | SqlExpr::IsDistinctFrom(_, _)
        | SqlExpr::IsNotDistinctFrom(_, _)
        | SqlExpr::ArrayOverlap(_, _)
        | SqlExpr::JsonbContains(_, _)
        | SqlExpr::JsonbContained(_, _)
        | SqlExpr::JsonbExists(_, _)
        | SqlExpr::JsonbExistsAny(_, _)
        | SqlExpr::JsonbExistsAll(_, _)
        | SqlExpr::JsonbPathExists(_, _)
        | SqlExpr::JsonbPathMatch(_, _)
        | SqlExpr::QuantifiedArray { .. } => SqlType::new(SqlTypeKind::Bool),
        SqlExpr::JsonGet(left, _) | SqlExpr::JsonPath(left, _) => {
            let left_type = infer_sql_expr_type(left, scope, catalog, outer_scopes, grouped_outer);
            if matches!(left_type.element_type().kind, SqlTypeKind::Jsonb) {
                SqlType::new(SqlTypeKind::Jsonb)
            } else {
                SqlType::new(SqlTypeKind::Json)
            }
        }
        SqlExpr::JsonGetText(_, _) | SqlExpr::JsonPathText(_, _) => {
            SqlType::new(SqlTypeKind::Text)
        }
        SqlExpr::AggCall { func, args, .. } => aggregate_sql_type(
            *func,
            args.first()
                .map(|expr| infer_sql_expr_type(expr, scope, catalog, outer_scopes, grouped_outer)),
        ),
        SqlExpr::ArrayLiteral(elements) => {
            infer_array_literal_type(elements, scope, catalog, outer_scopes, grouped_outer)
                .unwrap_or(SqlType::array_of(SqlType::new(SqlTypeKind::Text)))
        }
        SqlExpr::ScalarSubquery(select) => {
            build_plan_with_outer(select, catalog, outer_scopes, grouped_outer.cloned())
                .ok()
                .and_then(|plan| {
                    let cols = plan.columns();
                    if cols.len() == 1 {
                        Some(cols[0].sql_type)
                    } else {
                        None
                    }
                })
                .unwrap_or(SqlType::new(SqlTypeKind::Text))
        }
        SqlExpr::Exists(_) | SqlExpr::InSubquery { .. } | SqlExpr::QuantifiedSubquery { .. } => {
            SqlType::new(SqlTypeKind::Bool)
        }
        SqlExpr::Random => SqlType::new(SqlTypeKind::Float8),
        SqlExpr::FuncCall { name, .. } => match resolve_scalar_function(name) {
            Some(BuiltinScalarFunction::Random) => SqlType::new(SqlTypeKind::Float8),
            Some(BuiltinScalarFunction::ToJson)
            | Some(BuiltinScalarFunction::ArrayToJson)
            | Some(BuiltinScalarFunction::JsonBuildArray)
            | Some(BuiltinScalarFunction::JsonBuildObject)
            | Some(BuiltinScalarFunction::JsonObject) => SqlType::new(SqlTypeKind::Json),
            Some(BuiltinScalarFunction::ToJsonb)
            | Some(BuiltinScalarFunction::JsonbExtractPath)
            | Some(BuiltinScalarFunction::JsonbBuildArray)
            | Some(BuiltinScalarFunction::JsonbBuildObject)
            | Some(BuiltinScalarFunction::JsonbPathQueryArray)
            | Some(BuiltinScalarFunction::JsonbPathQueryFirst) => SqlType::new(SqlTypeKind::Jsonb),
            Some(BuiltinScalarFunction::GetDatabaseEncoding)
            | Some(BuiltinScalarFunction::JsonTypeof)
            | Some(BuiltinScalarFunction::JsonExtractPathText)
            | Some(BuiltinScalarFunction::JsonbTypeof)
            | Some(BuiltinScalarFunction::JsonbExtractPathText)
            | Some(BuiltinScalarFunction::Left)
            | Some(BuiltinScalarFunction::Repeat) => SqlType::new(SqlTypeKind::Text),
            Some(BuiltinScalarFunction::JsonArrayLength)
            | Some(BuiltinScalarFunction::JsonbArrayLength) => SqlType::new(SqlTypeKind::Int4),
            Some(BuiltinScalarFunction::JsonbPathExists)
            | Some(BuiltinScalarFunction::JsonbPathMatch) => SqlType::new(SqlTypeKind::Bool),
            Some(BuiltinScalarFunction::JsonExtractPath) => SqlType::new(SqlTypeKind::Json),
            None => SqlType::new(SqlTypeKind::Text),
        },
        SqlExpr::CurrentTimestamp => SqlType::new(SqlTypeKind::Timestamp),
    }
}

pub(super) fn bind_agg_output_expr(
    expr: &SqlExpr,
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlExpr>, bool)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    for (i, gk) in group_by_exprs.iter().enumerate() {
        if gk == expr {
            return Ok(Expr::Column(i));
        }
    }

    match expr {
        SqlExpr::AggCall {
            func,
            args,
            distinct,
        } => {
            let entry = (*func, args.clone(), *distinct);
            for (i, agg) in agg_list.iter().enumerate() {
                if *agg == entry {
                    return Ok(Expr::Column(n_keys + i));
                }
            }
            Err(ParseError::UnexpectedToken {
                expected: "known aggregate",
                actual: format!("{}(...)", func.name()),
            })
        }
        SqlExpr::Column(name) => {
            let col_index =
                match resolve_column_with_outer(input_scope, outer_scopes, name, grouped_outer)? {
                    ResolvedColumn::Local(index) => index,
                    ResolvedColumn::Outer { depth, index } => {
                        return Ok(Expr::OuterColumn { depth, index });
                    }
                };
            for (i, gk) in group_by_exprs.iter().enumerate() {
                if let SqlExpr::Column(gk_name) = gk {
                    if let Ok(gk_index) = resolve_column(input_scope, gk_name) {
                        if gk_index == col_index {
                            return Ok(Expr::Column(i));
                        }
                    }
                }
            }
            Err(ParseError::UngroupedColumn(name.clone()))
        }
        SqlExpr::Const(v) => Ok(Expr::Const(v.clone())),
        SqlExpr::IntegerLiteral(value) => Ok(Expr::Const(bind_integer_literal(value)?)),
        SqlExpr::NumericLiteral(value) => Ok(Expr::Const(bind_numeric_literal(value)?)),
        SqlExpr::Add(l, r) => Ok(Expr::Add(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Sub(l, r) => Ok(Expr::Sub(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Mul(l, r) => Ok(Expr::Mul(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Div(l, r) => Ok(Expr::Div(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Mod(l, r) => Ok(Expr::Mod(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Concat(l, r) => {
            let left_expr = bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?;
            let right_expr = bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?;
            let left_type = infer_sql_expr_type(l, input_scope, catalog, outer_scopes, grouped_outer);
            let right_type =
                infer_sql_expr_type(r, input_scope, catalog, outer_scopes, grouped_outer);
            bind_concat_operands(l, left_type, left_expr, r, right_type, right_expr)
        }
        SqlExpr::UnaryPlus(inner) => Ok(Expr::UnaryPlus(Box::new(bind_agg_output_expr(
            inner,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?))),
        SqlExpr::Negate(inner) => Ok(Expr::Negate(Box::new(bind_agg_output_expr(
            inner,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?))),
        SqlExpr::Cast(inner, ty) => {
            let bound_inner = if let SqlExpr::ArrayLiteral(elements) = inner.as_ref() {
                Expr::ArrayLiteral {
                    elements: elements
                        .iter()
                        .map(|element| {
                            bind_agg_output_expr(
                                element,
                                group_by_exprs,
                                input_scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                agg_list,
                                n_keys,
                            )
                        })
                        .collect::<Result<_, _>>()?,
                    array_type: *ty,
                }
            } else {
                bind_agg_output_expr(
                    inner,
                    group_by_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?
            };
            Ok(Expr::Cast(Box::new(bound_inner), *ty))
        }
        SqlExpr::Eq(l, r) => Ok(Expr::Eq(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::NotEq(l, r) => Ok(Expr::NotEq(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Lt(l, r) => Ok(Expr::Lt(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::LtEq(l, r) => Ok(Expr::LtEq(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Gt(l, r) => Ok(Expr::Gt(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::GtEq(l, r) => Ok(Expr::GtEq(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::RegexMatch(l, r) => Ok(Expr::RegexMatch(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::And(l, r) => Ok(Expr::And(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Or(l, r) => Ok(Expr::Or(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Not(inner) => Ok(Expr::Not(Box::new(bind_agg_output_expr(
            inner,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?))),
        SqlExpr::IsNull(inner) => Ok(Expr::IsNull(Box::new(bind_agg_output_expr(
            inner,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?))),
        SqlExpr::IsNotNull(inner) => Ok(Expr::IsNotNull(Box::new(bind_agg_output_expr(
            inner,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?))),
        SqlExpr::IsDistinctFrom(l, r) => Ok(Expr::IsDistinctFrom(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::IsNotDistinctFrom(l, r) => Ok(Expr::IsNotDistinctFrom(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::ArrayLiteral(elements) => Ok(Expr::ArrayLiteral {
            elements: elements
                .iter()
                .map(|element| {
                    bind_agg_output_expr(
                        element,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )
                })
                .collect::<Result<_, _>>()?,
            array_type: infer_array_literal_type(
                elements,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?,
        }),
        SqlExpr::ArrayOverlap(l, r) => Ok(Expr::ArrayOverlap(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonGet(l, r) => Ok(Expr::JsonGet(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonGetText(l, r) => Ok(Expr::JsonGetText(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonPath(l, r) => Ok(Expr::JsonPath(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonPathText(l, r) => Ok(Expr::JsonPathText(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbContains(l, r) => Ok(Expr::JsonbContains(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbContained(l, r) => Ok(Expr::JsonbContained(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbExists(l, r) => Ok(Expr::JsonbExists(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbExistsAny(l, r) => Ok(Expr::JsonbExistsAny(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbExistsAll(l, r) => Ok(Expr::JsonbExistsAll(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbPathExists(l, r) => Ok(Expr::JsonbPathExists(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbPathMatch(l, r) => Ok(Expr::JsonbPathMatch(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::ScalarSubquery(select) => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(input_scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            let plan = build_plan_with_outer(
                select,
                catalog,
                &child_outer,
                Some(GroupedOuterScope {
                    scope: input_scope.clone(),
                    group_by_exprs: group_by_exprs.to_vec(),
                }),
            )?;
            ensure_single_column_subquery(&plan)?;
            Ok(Expr::ScalarSubquery(Box::new(plan)))
        }
        SqlExpr::Exists(select) => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(input_scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            Ok(Expr::ExistsSubquery(Box::new(build_plan_with_outer(
                select,
                catalog,
                &child_outer,
                Some(GroupedOuterScope {
                    scope: input_scope.clone(),
                    group_by_exprs: group_by_exprs.to_vec(),
                }),
            )?)))
        }
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(input_scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            let subquery_plan = build_plan_with_outer(
                subquery,
                catalog,
                &child_outer,
                Some(GroupedOuterScope {
                    scope: input_scope.clone(),
                    group_by_exprs: group_by_exprs.to_vec(),
                }),
            )?;
            ensure_single_column_subquery(&subquery_plan)?;
            let any = Expr::AnySubquery {
                left: Box::new(bind_agg_output_expr(
                    expr,
                    group_by_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?),
                op: SubqueryComparisonOp::Eq,
                subquery: Box::new(subquery_plan),
            };
            if *negated {
                Ok(Expr::Not(Box::new(any)))
            } else {
                Ok(any)
            }
        }
        SqlExpr::QuantifiedSubquery {
            left,
            op,
            is_all,
            subquery,
        } => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(input_scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            let subquery_plan = build_plan_with_outer(
                subquery,
                catalog,
                &child_outer,
                Some(GroupedOuterScope {
                    scope: input_scope.clone(),
                    group_by_exprs: group_by_exprs.to_vec(),
                }),
            )?;
            ensure_single_column_subquery(&subquery_plan)?;
            if *is_all {
                Ok(Expr::AllSubquery {
                    left: Box::new(bind_agg_output_expr(
                        left,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                    op: *op,
                    subquery: Box::new(subquery_plan),
                })
            } else {
                Ok(Expr::AnySubquery {
                    left: Box::new(bind_agg_output_expr(
                        left,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                    op: *op,
                    subquery: Box::new(subquery_plan),
                })
            }
        }
        SqlExpr::QuantifiedArray {
            left,
            op,
            is_all,
            array,
        } => {
            if *is_all {
                Ok(Expr::AllArray {
                    left: Box::new(bind_agg_output_expr(
                        left,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                    op: *op,
                    right: Box::new(bind_agg_output_expr(
                        array,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                })
            } else {
                Ok(Expr::AnyArray {
                    left: Box::new(bind_agg_output_expr(
                        left,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                    op: *op,
                    right: Box::new(bind_agg_output_expr(
                        array,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                })
            }
        }
        SqlExpr::Random => Ok(Expr::Random),
        SqlExpr::FuncCall { name, args } => {
            let func =
                resolve_scalar_function(name).ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "supported builtin function",
                    actual: name.clone(),
                })?;
            validate_scalar_function_arity(func, args)?;
            let bound_args = args
                .iter()
                .map(|arg| {
                    bind_agg_output_expr(
                        arg,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            match func {
                BuiltinScalarFunction::Left | BuiltinScalarFunction::Repeat => {
                    let left_type =
                        infer_sql_expr_type(&args[0], input_scope, catalog, outer_scopes, grouped_outer);
                    let right_type =
                        infer_sql_expr_type(&args[1], input_scope, catalog, outer_scopes, grouped_outer);
                    if !should_use_text_concat(&args[0], left_type, &args[0], left_type) {
                        return Err(ParseError::UnexpectedToken {
                            expected: "text argument",
                            actual: format!("{func:?}({})", sql_type_name(left_type)),
                        });
                    }
                    if !is_numeric_family(right_type) {
                        return Err(ParseError::UnexpectedToken {
                            expected: "integer argument",
                            actual: format!("{func:?}({})", sql_type_name(right_type)),
                        });
                    }
                    Ok(Expr::FuncCall {
                        func,
                        args: vec![
                            coerce_bound_expr(
                                bound_args[0].clone(),
                                left_type,
                                SqlType::new(SqlTypeKind::Text),
                            ),
                            coerce_bound_expr(
                                bound_args[1].clone(),
                                right_type,
                                SqlType::new(SqlTypeKind::Int4),
                            ),
                        ],
                    })
                }
                _ => Ok(Expr::FuncCall {
                    func,
                    args: bound_args,
                }),
            }
        }
        SqlExpr::CurrentTimestamp => Ok(Expr::CurrentTimestamp),
    }
}

pub(super) fn infer_array_literal_type(
    elements: &[SqlExpr],
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<SqlType, ParseError> {
    for element in elements {
        if matches!(element, SqlExpr::Const(Value::Null)) {
            continue;
        }
        return Ok(SqlType::array_of(
            infer_sql_expr_type(element, scope, catalog, outer_scopes, grouped_outer)
                .element_type(),
        ));
    }
    Err(ParseError::UnexpectedToken {
        expected: "ARRAY[...] with a typed element or explicit cast",
        actual: "ARRAY[]".into(),
    })
}
