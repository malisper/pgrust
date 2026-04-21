use super::functions::*;
use super::infer::*;
use super::*;
use crate::backend::catalog::roles::find_role_by_name;
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::include::catalog::range_type_ref_for_sql_type;
use crate::include::nodes::primnodes::{
    BoolExprType, CaseExpr as BoundCaseExpr, CaseTestExpr as BoundCaseTestExpr,
    CaseWhen as BoundCaseWhen, ExprArraySubscript, INDEX_VAR, INNER_VAR, OUTER_VAR, OpExprKind,
    WindowFuncKind, expr_sql_type_hint,
};

mod func;
mod json;
mod ops;
mod subquery;
mod targets;

use self::func::{
    bind_row_to_json_call, bind_scalar_function_call, bind_user_defined_scalar_function_call,
};
use self::json::{
    bind_json_binary_expr, bind_jsonb_contained_expr, bind_jsonb_contains_expr,
    bind_jsonb_exists_all_expr, bind_jsonb_exists_any_expr, bind_jsonb_exists_expr,
    bind_jsonb_path_binary_expr, bind_maybe_jsonb_delete,
};
pub(crate) use self::ops::bind_concat_operands;
pub(super) use self::ops::bind_lowered_comparison_expr;
use self::ops::{
    bind_arithmetic_expr, bind_bitwise_expr, bind_bound_comparison_expr, bind_comparison_expr,
    bind_concat_expr, bind_overloaded_binary_expr, bind_prefix_operator_expr, bind_shift_expr,
};
use self::subquery::{
    bind_array_subquery_expr, bind_exists_subquery_expr, bind_in_subquery_expr,
    bind_quantified_array_expr, bind_quantified_subquery_expr, bind_scalar_subquery_expr,
};
pub(crate) use self::targets::{
    BoundSelectTargets, bind_select_targets, select_targets_contain_set_returning_call,
};
use super::multiranges::{
    bind_maybe_multirange_arithmetic, bind_maybe_multirange_comparison,
    bind_maybe_multirange_contains, bind_maybe_multirange_over_position,
    bind_maybe_multirange_shift,
};
use super::ranges::{
    bind_maybe_range_arithmetic, bind_maybe_range_comparison, bind_maybe_range_contains,
    bind_maybe_range_over_position, bind_maybe_range_shift,
};
use std::collections::BTreeSet;

fn supports_array_subscripts(array_type: SqlType) -> bool {
    array_type.is_array
        || matches!(
            array_type.kind,
            SqlTypeKind::Int2Vector | SqlTypeKind::OidVector
        )
}

#[allow(dead_code)]
pub(crate) fn bind_expr(expr: &SqlExpr, scope: &BoundScope) -> Result<Expr, ParseError> {
    bind_expr_with_outer(expr, scope, &Catalog::default(), &[], None)
}

pub(crate) fn bind_expr_with_outer(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Expr, ParseError> {
    bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, &[])
}

pub(super) fn raise_expr_varlevels(expr: Expr, levels: usize) -> Expr {
    if levels == 0 {
        return expr;
    }
    match expr {
        Expr::Var(mut var) => {
            if !matches!(var.varno, OUTER_VAR | INNER_VAR | INDEX_VAR) {
                var.varlevelsup += levels;
            }
            Expr::Var(var)
        }
        Expr::Aggref(mut aggref) => {
            aggref.agglevelsup += levels;
            Expr::Aggref(Box::new(crate::include::nodes::primnodes::Aggref {
                args: aggref
                    .args
                    .into_iter()
                    .map(|arg| raise_expr_varlevels(arg, levels))
                    .collect(),
                aggorder: aggref
                    .aggorder
                    .into_iter()
                    .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                        expr: raise_expr_varlevels(item.expr, levels),
                        ..item
                    })
                    .collect(),
                aggfilter: aggref
                    .aggfilter
                    .map(|expr| raise_expr_varlevels(expr, levels)),
                ..*aggref
            }))
        }
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, levels))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, levels))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, levels))
                .collect(),
            ..*func
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(raise_expr_varlevels(*saop.left, levels)),
                right: Box::new(raise_expr_varlevels(*saop.right, levels)),
                ..*saop
            },
        )),
        Expr::Xml(xml) => Expr::Xml(Box::new(crate::include::nodes::primnodes::XmlExpr {
            op: xml.op,
            name: xml.name,
            named_args: xml
                .named_args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, levels))
                .collect(),
            arg_names: xml.arg_names,
            args: xml
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, levels))
                .collect(),
            xml_option: xml.xml_option,
            indent: xml.indent,
            target_type: xml.target_type,
            standalone: xml.standalone,
        })),
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(raise_expr_varlevels(*inner, levels)), ty),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(raise_expr_varlevels(*inner, levels))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(raise_expr_varlevels(*inner, levels))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(raise_expr_varlevels(*left, levels)),
            Box::new(raise_expr_varlevels(*right, levels)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(raise_expr_varlevels(*left, levels)),
            Box::new(raise_expr_varlevels(*right, levels)),
        ),
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(raise_expr_varlevels(*left, levels)),
            Box::new(raise_expr_varlevels(*right, levels)),
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(raise_expr_varlevels(*expr, levels)),
            pattern: Box::new(raise_expr_varlevels(*pattern, levels)),
            escape: escape.map(|expr| Box::new(raise_expr_varlevels(*expr, levels))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(raise_expr_varlevels(*expr, levels)),
            pattern: Box::new(raise_expr_varlevels(*pattern, levels)),
            escape: escape.map(|expr| Box::new(raise_expr_varlevels(*expr, levels))),
            negated,
        },
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| raise_expr_varlevels(element, levels))
                .collect(),
            array_type,
        },
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, raise_expr_varlevels(expr, levels)))
                .collect(),
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(raise_expr_varlevels(*expr, levels)),
            field,
            field_type,
        },
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(raise_expr_varlevels(*arg, levels))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: raise_expr_varlevels(arm.expr, levels),
                    result: raise_expr_varlevels(arm.result, levels),
                })
                .collect(),
            defresult: Box::new(raise_expr_varlevels(*case_expr.defresult, levels)),
            ..*case_expr
        })),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(raise_expr_varlevels(*array, levels)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| raise_expr_varlevels(expr, levels)),
                    upper: subscript
                        .upper
                        .map(|expr| raise_expr_varlevels(expr, levels)),
                })
                .collect(),
        },
        other => other,
    }
}

fn current_window_state_or_error()
-> Result<std::rc::Rc<std::cell::RefCell<WindowBindingState>>, ParseError> {
    match current_window_state() {
        Some(state) if windows_allowed() => Ok(state),
        Some(_) => Err(nested_window_error()),
        None => Err(window_not_allowed_error()),
    }
}

fn bind_window_agg_call(
    func: AggFunc,
    args: &[SqlFunctionArg],
    order_by: &[OrderByItem],
    distinct: bool,
    func_variadic: bool,
    filter: Option<&SqlExpr>,
    over: &RawWindowSpec,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let state = current_window_state_or_error()?;
    if aggregate_args_are_named(args) {
        return Err(ParseError::UnexpectedToken {
            expected: "aggregate arguments without names",
            actual: func.name().into(),
        });
    }
    let arg_values = args.iter().map(|arg| arg.value.clone()).collect::<Vec<_>>();
    validate_aggregate_arity(func, &arg_values)?;
    let arg_types = arg_values
        .iter()
        .map(|expr| {
            infer_sql_expr_type_with_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)
        })
        .collect::<Vec<_>>();
    let resolved = resolve_aggregate_call(catalog, func, &arg_types, func_variadic);
    let bound_args = arg_values
        .iter()
        .map(|expr| {
            with_windows_disallowed(|| {
                bind_expr_with_outer_and_ctes(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let coerced_args = if let Some(resolved) = &resolved {
        bound_args
            .into_iter()
            .zip(arg_types.iter().copied())
            .zip(resolved.declared_arg_types.iter().copied())
            .map(|((arg, actual_type), declared_type)| {
                coerce_bound_expr(arg, actual_type, declared_type)
            })
            .collect()
    } else {
        bound_args
    };
    let bound_filter = filter
        .map(|expr| {
            with_windows_disallowed(|| {
                bind_expr_with_outer_and_ctes(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            })
        })
        .transpose()?;
    let bound_order_by = order_by
        .iter()
        .map(|item| {
            Ok(OrderByEntry {
                expr: bind_expr_with_outer_and_ctes(
                    &item.expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                ressortgroupref: 0,
                descending: item.descending,
                nulls_first: item.nulls_first,
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    for item in &bound_order_by {
        reject_nested_local_ctes_in_agg_expr(&item.expr)?;
    }
    let spec = bind_window_spec(over, |expr| {
        bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)
    })?;
    let kind = WindowFuncKind::Aggregate(crate::include::nodes::primnodes::Aggref {
        aggfnoid: resolved
            .as_ref()
            .map(|call| call.proc_oid)
            .or_else(|| proc_oid_for_builtin_aggregate_function(func))
            .unwrap_or(0),
        aggtype: aggregate_sql_type(func, arg_types.first().copied()),
        aggvariadic: resolved
            .as_ref()
            .map(|call| call.func_variadic)
            .unwrap_or(func_variadic),
        aggdistinct: distinct,
        args: coerced_args.clone(),
        aggorder: bound_order_by,
        aggfilter: bound_filter,
        agglevelsup: 0,
        aggno: 0,
    });
    Ok(register_window_expr(
        &state,
        spec,
        kind,
        coerced_args,
        aggregate_sql_type(func, arg_types.first().copied()),
    ))
}

fn bind_window_func_call(
    name: &str,
    args: &[SqlFunctionArg],
    func_variadic: bool,
    over: &RawWindowSpec,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let state = current_window_state_or_error()?;
    let actual_types = args
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
    let mut resolution_types = actual_types.clone();
    if matches!(args.len(), 3)
        && !func_variadic
        && (name.eq_ignore_ascii_case("lag") || name.eq_ignore_ascii_case("lead"))
    {
        let common_type = infer_common_scalar_expr_type_with_ctes(
            &[args[0].value.clone(), args[2].value.clone()],
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
            "lag/lead value and default arguments with a common type",
        )?;
        resolution_types[0] = common_type;
        resolution_types[2] = common_type;
    }
    let resolved = resolve_function_call(catalog, name, &resolution_types, func_variadic)?;
    if resolved.proretset || !matches!(resolved.prokind, 'w' | 'a') {
        return Err(ParseError::UnexpectedToken {
            expected: "window or aggregate function",
            actual: name.to_string(),
        });
    }
    let spec = bind_window_spec(over, |expr| {
        bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)
    })?;
    if let Some(window_impl) = resolved.window_impl {
        if args.iter().any(|arg| arg.name.is_some()) {
            return Err(ParseError::FeatureNotSupported(
                "named arguments are not supported for window functions".into(),
            ));
        }
        let bound_args = args
            .iter()
            .map(|arg| {
                with_windows_disallowed(|| {
                    bind_expr_with_outer_and_ctes(
                        &arg.value,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let coerced_args = bound_args
            .into_iter()
            .zip(actual_types.iter().copied())
            .zip(resolved.declared_arg_types.iter().copied())
            .map(|((arg, actual_type), declared_type)| {
                coerce_bound_expr(arg, actual_type, declared_type)
            })
            .collect::<Vec<_>>();
        return Ok(register_window_expr(
            &state,
            spec,
            WindowFuncKind::Builtin(window_impl),
            coerced_args,
            resolved.result_type,
        ));
    }
    if let Some(agg_impl) = resolved.agg_impl {
        return bind_window_agg_call(
            agg_impl,
            args,
            &[],
            false,
            resolved.func_variadic,
            None,
            over,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
    }
    Err(ParseError::FeatureNotSupported(format!(
        "window function {name}"
    )))
}

pub(crate) fn bind_expr_with_outer_and_ctes(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    Ok(match expr {
        SqlExpr::Xml(xml) => {
            return bind_xml_expr(xml, scope, catalog, outer_scopes, grouped_outer, ctes);
        }
        SqlExpr::Column(name) => {
            if let Some(relation_name) = name.strip_suffix(".*") {
                let fields =
                    resolve_relation_row_expr_with_outer(scope, outer_scopes, relation_name)
                        .ok_or_else(|| ParseError::UnknownColumn(name.clone()))?;
                Expr::Row {
                    descriptor: assign_anonymous_record_descriptor(
                        fields
                            .iter()
                            .map(|(field_name, expr)| {
                                (
                                    field_name.clone(),
                                    expr_sql_type_hint(expr)
                                        .unwrap_or(SqlType::new(SqlTypeKind::Text)),
                                )
                            })
                            .collect(),
                    ),
                    fields,
                }
            } else if let Some(system_column) =
                resolve_system_column_with_outer(scope, outer_scopes, name)?
            {
                Expr::Var(crate::include::nodes::primnodes::Var {
                    varno: system_column.varno,
                    varattno: system_column.varattno,
                    varlevelsup: system_column.varlevelsup,
                    vartype: system_column.sql_type,
                })
            } else {
                match resolve_column_with_outer(scope, outer_scopes, name, grouped_outer)? {
                    ResolvedColumn::Local(index) => scope.output_exprs.get(index).cloned().unwrap_or_else(|| {
                        panic!("bound scope output_exprs missing local column {index} for {name}")
                    }),
                    ResolvedColumn::Outer { depth, index } => outer_scopes
                        .get(depth)
                        .and_then(|scope| scope.output_exprs.get(index))
                        .cloned()
                        .map(|expr| raise_expr_varlevels(expr, depth + 1))
                        .unwrap_or_else(|| {
                            panic!(
                                "outer scope output_exprs missing outer column depth={} index={} for {}",
                                depth, index, name
                            )
                        }),
                }
            }
        }
        SqlExpr::Default => {
            return Err(ParseError::UnexpectedToken {
                expected: "expression",
                actual: "DEFAULT".into(),
            });
        }
        SqlExpr::Const(value) => Expr::Const(value.clone()),
        SqlExpr::IntegerLiteral(value) => Expr::Const(bind_integer_literal(value)?),
        SqlExpr::NumericLiteral(value) => Expr::Const(bind_numeric_literal(value)?),
        SqlExpr::Row(items) => {
            let mut field_exprs = Vec::new();
            for item in items {
                if let SqlExpr::Column(name) = item
                    && let Some(relation_name) = name.strip_suffix(".*")
                {
                    let fields =
                        resolve_relation_row_expr_with_outer(scope, outer_scopes, relation_name)
                            .ok_or_else(|| ParseError::UnknownColumn(name.clone()))?;
                    for (_, expr) in fields {
                        let field_name = format!("f{}", field_exprs.len() + 1);
                        field_exprs.push((field_name, expr));
                    }
                    continue;
                }
                let expr = bind_expr_with_outer_and_ctes(
                    item,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let field_name = format!("f{}", field_exprs.len() + 1);
                field_exprs.push((field_name, expr));
            }
            let descriptor = assign_anonymous_record_descriptor(
                field_exprs
                    .iter()
                    .map(|(name, expr)| {
                        (
                            name.clone(),
                            expr_sql_type_hint(expr).unwrap_or(SqlType::new(SqlTypeKind::Text)),
                        )
                    })
                    .collect(),
            );
            Expr::Row {
                descriptor,
                fields: field_exprs,
            }
        }
        SqlExpr::BinaryOperator { op, left, right } => match op.as_str() {
            "@@" => bind_overloaded_binary_expr(
                "@@",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "&&" => bind_overloaded_binary_expr(
                "&&",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            "-|-" => bind_overloaded_binary_expr(
                "-|-",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "bound builtin operator",
                    actual: format!("unsupported operator {op}"),
                });
            }
        },
        SqlExpr::Add(left, right) => {
            if let Some(result) = bind_maybe_multirange_arithmetic(
                "+",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_arithmetic(
                "+",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_arithmetic(
                "+",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_arithmetic_expr(
                    "+",
                    OpExprKind::Add,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Sub(left, right) => {
            if let Some(result) = bind_maybe_jsonb_delete(
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_multirange_arithmetic(
                "-",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_arithmetic(
                "-",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_arithmetic(
                "-",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_arithmetic_expr(
                    "-",
                    OpExprKind::Sub,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::BitAnd(left, right) => bind_bitwise_expr(
            "&",
            OpExprKind::BitAnd,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::BitOr(left, right) => bind_bitwise_expr(
            "|",
            OpExprKind::BitOr,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::BitXor(left, right) => {
            if let Some(result) = bind_maybe_geometry_arithmetic(
                "#",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_bitwise_expr(
                    "#",
                    OpExprKind::BitXor,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Shl(left, right) => {
            if let Some(result) = bind_maybe_multirange_shift(
                "<<",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_shift(
                "<<",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_shift(
                "<<",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_shift_expr(
                    "<<",
                    OpExprKind::Shl,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Shr(left, right) => {
            if let Some(result) = bind_maybe_multirange_shift(
                ">>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_shift(
                ">>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_shift(
                ">>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_shift_expr(
                    ">>",
                    OpExprKind::Shr,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Mul(left, right) => {
            if let Some(result) = bind_maybe_multirange_arithmetic(
                "*",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_arithmetic(
                "*",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_arithmetic(
                "*",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_arithmetic_expr(
                    "*",
                    OpExprKind::Mul,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Div(left, right) => {
            if let Some(result) = bind_maybe_geometry_arithmetic(
                "/",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_arithmetic_expr(
                    "/",
                    OpExprKind::Div,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Mod(left, right) => bind_arithmetic_expr(
            "%",
            OpExprKind::Mod,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::Concat(left, right) => bind_concat_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::UnaryPlus(inner) => Expr::op_auto(
            OpExprKind::UnaryPlus,
            vec![bind_expr_with_outer_and_ctes(
                inner,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?],
        ),
        SqlExpr::PrefixOperator { op, expr } => bind_prefix_operator_expr(
            op.as_str(),
            expr,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::Negate(inner) => Expr::op_auto(
            OpExprKind::Negate,
            vec![bind_expr_with_outer_and_ctes(
                inner,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?],
        ),
        SqlExpr::BitNot(inner) => {
            let inner_type = infer_sql_expr_type_with_ctes(
                inner,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !is_integer_family(inner_type) && !is_bit_string_type(inner_type) {
                return Err(ParseError::UndefinedOperator {
                    op: "~",
                    left_type: sql_type_name(inner_type),
                    right_type: "unknown".to_string(),
                });
            }
            Expr::op_auto(
                OpExprKind::BitNot,
                vec![bind_expr_with_outer_and_ctes(
                    inner,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?],
            )
        }
        SqlExpr::Cast(inner, ty) => {
            let source_type = infer_sql_expr_type_with_ctes(
                inner,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let bound_inner = if let SqlExpr::ArrayLiteral(elements) = inner.as_ref() {
                Expr::ArrayLiteral {
                    elements: elements
                        .iter()
                        .map(|element| {
                            bind_expr_with_outer_and_ctes(
                                element,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            )
                        })
                        .collect::<Result<_, _>>()?,
                    array_type: raw_type_name_hint(ty),
                }
            } else {
                bind_expr_with_outer_and_ctes(
                    inner,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            };
            let target_type = resolve_raw_type_name(ty, catalog)?;
            if target_type.kind == SqlTypeKind::RegRole
                && let Some(bound_regrole) = bind_regrole_literal_cast(inner, target_type, catalog)?
            {
                return Ok(bound_regrole);
            }
            if target_type.kind == SqlTypeKind::RegProcedure
                && let Some(bound_regprocedure) =
                    bind_regprocedure_literal_cast(inner, target_type, catalog)?
            {
                return Ok(bound_regprocedure);
            }
            if !matches!(inner.as_ref(), SqlExpr::Const(Value::Null)) {
                validate_catalog_backed_explicit_cast(source_type, target_type, catalog)?;
            }
            coerce_bound_expr(bound_inner, source_type, target_type)
        }
        SqlExpr::Eq(left, right) => {
            if let Some(result) = bind_maybe_multirange_comparison(
                "=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_comparison(
                "=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_comparison(
                "=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    "=",
                    OpExprKind::Eq,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::NotEq(left, right) => {
            if let Some(result) = bind_maybe_multirange_comparison(
                "<>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_comparison(
                "<>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_comparison(
                "<>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    "<>",
                    OpExprKind::NotEq,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Lt(left, right) => {
            if let Some(result) = bind_maybe_multirange_comparison(
                "<",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_comparison(
                "<",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_comparison(
                "<",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    "<",
                    OpExprKind::Lt,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::LtEq(left, right) => {
            if let Some(result) = bind_maybe_multirange_comparison(
                "<=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_comparison(
                "<=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_comparison(
                "<=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    "<=",
                    OpExprKind::LtEq,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Gt(left, right) => {
            if let Some(result) = bind_maybe_multirange_comparison(
                ">",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_comparison(
                ">",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_comparison(
                ">",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    ">",
                    OpExprKind::Gt,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::GtEq(left, right) => {
            if let Some(result) = bind_maybe_multirange_comparison(
                ">=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_comparison(
                ">=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_geometry_comparison(
                ">=",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_comparison_expr(
                    ">=",
                    OpExprKind::GtEq,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::RegexMatch(left, right) => Expr::op_auto(
            OpExprKind::RegexMatch,
            vec![
                bind_expr_with_outer_and_ctes(
                    left,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                bind_expr_with_outer_and_ctes(
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
            ],
        ),
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(bind_expr_with_outer_and_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            pattern: Box::new(bind_expr_with_outer_and_ctes(
                pattern,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            escape: match escape {
                Some(value) => Some(Box::new(bind_expr_with_outer_and_ctes(
                    value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?)),
                None => None,
            },
            case_insensitive: *case_insensitive,
            negated: *negated,
        },
        SqlExpr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(bind_expr_with_outer_and_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            pattern: Box::new(bind_expr_with_outer_and_ctes(
                pattern,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            escape: match escape {
                Some(value) => Some(Box::new(bind_expr_with_outer_and_ctes(
                    value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?)),
                None => None,
            },
            negated: *negated,
        },
        SqlExpr::And(left, right) => Expr::bool_expr(
            BoolExprType::And,
            vec![
                bind_expr_with_outer_and_ctes(
                    left,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                bind_expr_with_outer_and_ctes(
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
            ],
        ),
        SqlExpr::Or(left, right) => Expr::bool_expr(
            BoolExprType::Or,
            vec![
                bind_expr_with_outer_and_ctes(
                    left,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                bind_expr_with_outer_and_ctes(
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
            ],
        ),
        SqlExpr::Not(inner) => Expr::bool_expr(
            BoolExprType::Not,
            vec![bind_expr_with_outer_and_ctes(
                inner,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?],
        ),
        SqlExpr::IsNull(inner) => Expr::IsNull(Box::new(bind_expr_with_outer_and_ctes(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?)),
        SqlExpr::IsNotNull(inner) => Expr::IsNotNull(Box::new(bind_expr_with_outer_and_ctes(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?)),
        SqlExpr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(bind_expr_with_outer_and_ctes(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Box::new(bind_expr_with_outer_and_ctes(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
        ),
        SqlExpr::ArrayLiteral(elements) => Expr::ArrayLiteral {
            elements: elements
                .iter()
                .map(|element| {
                    bind_expr_with_outer_and_ctes(
                        element,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                })
                .collect::<Result<_, _>>()?,
            array_type: infer_array_literal_type_with_ctes(
                elements,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "ARRAY[...] with a typed element or explicit cast",
                actual: "ARRAY[]".into(),
            })?,
        },
        SqlExpr::ArraySubscript { array, subscripts } => {
            let array_type = infer_sql_expr_type_with_ctes(
                array,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if array_type.kind == SqlTypeKind::Point
                && subscripts.iter().any(|subscript| subscript.is_slice)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "array expression",
                    actual: "point".into(),
                });
            }
            if !supports_array_subscripts(array_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "array expression",
                    actual: sql_type_name(array_type).into(),
                });
            }
            Expr::ArraySubscript {
                array: Box::new(bind_expr_with_outer_and_ctes(
                    array,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?),
                subscripts: subscripts
                    .iter()
                    .map(|subscript| {
                        Ok(crate::include::nodes::primnodes::ExprArraySubscript {
                            is_slice: subscript.is_slice,
                            lower: subscript
                                .lower
                                .as_deref()
                                .map(|expr| {
                                    bind_expr_with_outer_and_ctes(
                                        expr,
                                        scope,
                                        catalog,
                                        outer_scopes,
                                        grouped_outer,
                                        ctes,
                                    )
                                })
                                .transpose()?,
                            upper: subscript
                                .upper
                                .as_deref()
                                .map(|expr| {
                                    bind_expr_with_outer_and_ctes(
                                        expr,
                                        scope,
                                        catalog,
                                        outer_scopes,
                                        grouped_outer,
                                        ctes,
                                    )
                                })
                                .transpose()?,
                        })
                    })
                    .collect::<Result<_, ParseError>>()?,
            }
        }
        SqlExpr::ArrayOverlap(left, right) => {
            if let Some(result) = bind_maybe_geometry_comparison(
                "&&",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                let raw_left_type = infer_sql_expr_type_with_ctes(
                    left,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                let raw_right_type = infer_sql_expr_type_with_ctes(
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                let left_bound = bind_expr_with_outer_and_ctes(
                    left,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let right_bound = bind_expr_with_outer_and_ctes(
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let mut left_type =
                    coerce_unknown_string_literal_type(left, raw_left_type, raw_right_type);
                let mut right_type =
                    coerce_unknown_string_literal_type(right, raw_right_type, left_type);
                let left_expr = if matches!(
                    &**left,
                    SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
                ) && !left_type.is_array
                {
                    if let Expr::ArrayLiteral { array_type, .. } = &right_bound {
                        left_type = *array_type;
                    }
                    coerce_bound_expr(left_bound, raw_left_type, left_type)
                } else {
                    coerce_bound_expr(left_bound, raw_left_type, left_type)
                };
                let right_expr = if matches!(
                    &**right,
                    SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
                ) && !right_type.is_array
                {
                    if let Expr::ArrayLiteral { array_type, .. } = &left_expr {
                        right_type = *array_type;
                    }
                    coerce_bound_expr(right_bound, raw_right_type, right_type)
                } else {
                    coerce_bound_expr(right_bound, raw_right_type, right_type)
                };
                Expr::op_auto(OpExprKind::ArrayOverlap, vec![left_expr, right_expr])
            }
        }
        SqlExpr::ScalarSubquery(select) => {
            bind_scalar_subquery_expr(select, scope, catalog, outer_scopes, ctes)?
        }
        SqlExpr::ArraySubquery(select) => {
            bind_array_subquery_expr(select, scope, catalog, outer_scopes, ctes)?
        }
        SqlExpr::Exists(select) => {
            bind_exists_subquery_expr(select, scope, catalog, outer_scopes, ctes)?
        }
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => bind_in_subquery_expr(
            expr,
            subquery,
            *negated,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::QuantifiedSubquery {
            left,
            op,
            is_all,
            subquery,
        } => bind_quantified_subquery_expr(
            left,
            *op,
            *is_all,
            subquery,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::QuantifiedArray {
            left,
            op,
            is_all,
            array,
        } => bind_quantified_array_expr(
            left,
            *op,
            *is_all,
            array,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::Random => Expr::Random,
        SqlExpr::JsonGet(left, right) => bind_json_binary_expr(
            OpExprKind::JsonGet,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonGetText(left, right) => bind_json_binary_expr(
            OpExprKind::JsonGetText,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonPath(left, right) => bind_json_binary_expr(
            OpExprKind::JsonPath,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonPathText(left, right) => bind_json_binary_expr(
            OpExprKind::JsonPathText,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbContains(left, right) => {
            if let Some(result) = bind_maybe_multirange_contains(
                "@>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_contains(
                "@>",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_jsonb_contains_expr(
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::JsonbContained(left, right) => {
            if let Some(result) = bind_maybe_multirange_contains(
                "<@",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else if let Some(result) = bind_maybe_range_contains(
                "<@",
                left,
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) {
                result?
            } else {
                bind_jsonb_contained_expr(
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::JsonbExists(left, right) => bind_jsonb_exists_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbExistsAny(left, right) => bind_jsonb_exists_any_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbExistsAll(left, right) => bind_jsonb_exists_all_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbPathExists(left, right) => bind_jsonb_path_binary_expr(
            OpExprKind::JsonbPathExists,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::JsonbPathMatch(left, right) => bind_jsonb_path_binary_expr(
            OpExprKind::JsonbPathMatch,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            distinct,
            func_variadic,
            filter,
            over,
        } => {
            let args_list = args.args();
            if let Some(func) = resolve_builtin_aggregate(name) {
                if let Some(raw_over) = over {
                    return bind_window_agg_call(
                        func,
                        args_list,
                        order_by,
                        *distinct,
                        *func_variadic,
                        filter.as_deref(),
                        raw_over,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                }
                return Err(ParseError::UnexpectedToken {
                    expected: "non-aggregate expression",
                    actual: "aggregate function".into(),
                });
            }
            if let Some(raw_over) = over {
                return bind_window_func_call(
                    name,
                    args_list,
                    *func_variadic,
                    raw_over,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if name.eq_ignore_ascii_case("row_to_json") {
                return bind_row_to_json_call(
                    name,
                    args_list,
                    *func_variadic,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if name.eq_ignore_ascii_case("coalesce") {
                return bind_coalesce_call(
                    args_list,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if name.eq_ignore_ascii_case("nullif") {
                return bind_nullif_call(
                    args_list,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if !order_by.is_empty() || *distinct || filter.is_some() || args.is_star() {
                return Err(ParseError::UnexpectedToken {
                    expected: "supported scalar function",
                    actual: name.clone(),
                });
            }
            if !*func_variadic
                && let Some(target_type) = resolve_function_cast_type(catalog, name)
                && args_list.len() == 1
                && args_list.iter().all(|arg| arg.name.is_none())
            {
                let arg_type = infer_sql_expr_type_with_ctes(
                    &args_list[0].value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                let bound_arg = bind_expr_with_outer_and_ctes(
                    &args_list[0].value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                validate_catalog_backed_explicit_cast(arg_type, target_type, catalog)?;
                return Ok(Expr::Cast(
                    Box::new(bound_arg),
                    if arg_type == target_type {
                        arg_type
                    } else {
                        target_type
                    },
                ));
            }
            let actual_types = args_list
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
            let mut resolution_types = actual_types.clone();
            if matches!(args_list.len(), 3)
                && !*func_variadic
                && (name.eq_ignore_ascii_case("lag") || name.eq_ignore_ascii_case("lead"))
            {
                let common_type = infer_common_scalar_expr_type_with_ctes(
                    &[args_list[0].value.clone(), args_list[2].value.clone()],
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                    "lag/lead value and default arguments with a common type",
                )?;
                resolution_types[0] = common_type;
                resolution_types[2] = common_type;
            }
            if let Ok(resolved) =
                resolve_function_call(catalog, name, &resolution_types, *func_variadic)
            {
                if resolved.window_impl.is_some() {
                    return Err(window_function_requires_over_error(name));
                }
                if resolved.prokind != 'f' || resolved.proretset {
                    return Err(ParseError::UnexpectedToken {
                        expected: "supported scalar function",
                        actual: name.clone(),
                    });
                }
                if let Some(func) = resolved.scalar_impl {
                    let lowered_args = lower_named_scalar_function_args(func, args_list)?;
                    return bind_scalar_function_call(
                        func,
                        resolved.proc_oid,
                        Some(resolved.result_type),
                        resolved.func_variadic,
                        resolved.nvargs,
                        resolved.vatype_oid,
                        &resolved.declared_arg_types,
                        &lowered_args,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                }
                return bind_user_defined_scalar_function_call(
                    resolved.proc_oid,
                    resolved.result_type,
                    &resolved.declared_arg_types,
                    args_list,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if name.eq_ignore_ascii_case("xmlconcat") {
                if args.args().iter().any(|arg| arg.name.is_some()) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "positional xmlconcat arguments",
                        actual: "named argument".into(),
                    });
                }
                let xml_type = SqlType::new(SqlTypeKind::Xml);
                let bound_args = args
                    .args()
                    .iter()
                    .map(|arg| {
                        let source = infer_sql_expr_type_with_ctes(
                            &arg.value,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        );
                        Ok(coerce_bound_expr(
                            bind_expr_with_outer_and_ctes(
                                &arg.value,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            )?,
                            source,
                            xml_type,
                        ))
                    })
                    .collect::<Result<Vec<_>, ParseError>>()?;
                return Ok(Expr::Xml(Box::new(
                    crate::include::nodes::primnodes::XmlExpr {
                        op: crate::include::nodes::primnodes::XmlExprOp::Concat,
                        name: None,
                        named_args: Vec::new(),
                        arg_names: Vec::new(),
                        args: bound_args,
                        xml_option: None,
                        indent: None,
                        target_type: None,
                        standalone: None,
                    },
                )));
            }
            let legacy_func =
                resolve_scalar_function(name).ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "supported builtin function",
                    actual: name.clone(),
                })?;
            let lowered_args = lower_named_scalar_function_args(legacy_func, args_list)?;
            let actual_types = lowered_args
                .iter()
                .map(|arg| {
                    infer_sql_expr_type_with_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                })
                .collect::<Vec<_>>();
            validate_scalar_function_arity(legacy_func, &lowered_args)?;
            let legacy_result_type =
                if matches!(legacy_func, BuiltinScalarFunction::RangeConstructor) {
                    resolve_function_cast_type(catalog, name)
                        .filter(|ty| range_type_ref_for_sql_type(*ty).is_some())
                } else {
                    None
                };
            let legacy_declared_arg_types = if let Some(range_type) =
                legacy_result_type.and_then(range_type_ref_for_sql_type)
            {
                let mut declared = vec![range_type.subtype, range_type.subtype];
                if lowered_args.len() == 3 {
                    declared.push(SqlType::new(SqlTypeKind::Text));
                }
                declared
            } else {
                actual_types.clone()
            };
            bind_scalar_function_call(
                legacy_func,
                0,
                legacy_result_type,
                false,
                0,
                0,
                &legacy_declared_arg_types,
                &lowered_args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?
        }
        SqlExpr::Subscript { expr, index } => bind_geometry_subscript(
            expr,
            *index,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::GeometryUnaryOp { op, expr } => {
            bind_geometry_unary_expr(*op, expr, scope, catalog, outer_scopes, grouped_outer, ctes)?
        }
        SqlExpr::GeometryBinaryOp { op, left, right } => {
            if matches!(op, GeometryBinaryOp::OverLeft | GeometryBinaryOp::OverRight) {
                let range_op = if matches!(op, GeometryBinaryOp::OverLeft) {
                    "&<"
                } else {
                    "&>"
                };
                if let Some(result) = bind_maybe_multirange_over_position(
                    range_op,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                ) {
                    result?
                } else if let Some(result) = bind_maybe_range_over_position(
                    range_op,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                ) {
                    result?
                } else {
                    bind_geometry_binary_expr(
                        *op,
                        left,
                        right,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?
                }
            } else {
                bind_geometry_binary_expr(
                    *op,
                    left,
                    right,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            }
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => bind_case_expr(
            arg.as_deref(),
            args,
            defresult.as_deref(),
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::FieldSelect { expr, field } => bind_field_select_expr(
            expr,
            field,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        SqlExpr::CurrentDate => Expr::CurrentDate,
        SqlExpr::CurrentUser => Expr::CurrentUser,
        SqlExpr::SessionUser => Expr::SessionUser,
        SqlExpr::CurrentRole => Expr::CurrentRole,
        SqlExpr::CurrentTime { precision } => Expr::CurrentTime {
            precision: *precision,
        },
        SqlExpr::CurrentTimestamp { precision } => Expr::CurrentTimestamp {
            precision: *precision,
        },
        SqlExpr::LocalTime { precision } => Expr::LocalTime {
            precision: *precision,
        },
        SqlExpr::LocalTimestamp { precision } => Expr::LocalTimestamp {
            precision: *precision,
        },
    })
}

fn bind_field_select_expr(
    expr: &SqlExpr,
    field: &str,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let bound_inner = match expr {
        SqlExpr::Column(name)
            if resolve_relation_row_expr_with_outer(scope, outer_scopes, name).is_some() =>
        {
            let fields = resolve_relation_row_expr_with_outer(scope, outer_scopes, name)
                .expect("checked above");
            Expr::Row {
                descriptor: assign_anonymous_record_descriptor(
                    fields
                        .iter()
                        .map(|(field_name, expr)| {
                            (
                                field_name.clone(),
                                expr_sql_type_hint(expr).unwrap_or(SqlType::new(SqlTypeKind::Text)),
                            )
                        })
                        .collect(),
                ),
                fields,
            }
        }
        _ => {
            bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)?
        }
    };
    let field_type = resolve_bound_field_select_type(&bound_inner, field, catalog)?;
    Ok(Expr::FieldSelect {
        expr: Box::new(bound_inner),
        field: field.to_string(),
        field_type,
    })
}

fn resolve_bound_field_select_type(
    expr: &Expr,
    field: &str,
    catalog: &dyn CatalogLookup,
) -> Result<SqlType, ParseError> {
    if let Expr::Row { descriptor, .. } = expr {
        if let Some(found) = descriptor
            .fields
            .iter()
            .find(|candidate| candidate.name.eq_ignore_ascii_case(field))
        {
            return Ok(found.sql_type);
        }
    }

    let Some(row_type) = expr_sql_type_hint(expr) else {
        return Err(ParseError::UnexpectedToken {
            expected: "record expression",
            actual: format!("field selection .{field}"),
        });
    };

    if matches!(row_type.kind, SqlTypeKind::Composite) && row_type.typrelid != 0 {
        let relation = catalog
            .lookup_relation_by_oid(row_type.typrelid)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "named composite type",
                actual: format!("type relation {} not found", row_type.typrelid),
            })?;
        if let Some(found) = relation
            .desc
            .columns
            .iter()
            .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(field))
        {
            return Ok(found.sql_type);
        }
    }

    Err(ParseError::UnexpectedToken {
        expected: "record field",
        actual: format!("field selection .{field}"),
    })
}

fn bind_case_expr(
    arg: Option<&SqlExpr>,
    args: &[SqlCaseWhen],
    defresult: Option<&SqlExpr>,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if args.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "at least one WHEN clause",
            actual: "CASE".into(),
        });
    }

    let default_sql_expr = SqlExpr::Const(Value::Null);
    let default_expr = defresult.unwrap_or(&default_sql_expr);
    let mut result_exprs = Vec::with_capacity(args.len() + 1);
    result_exprs.push(default_expr.clone());
    result_exprs.extend(args.iter().map(|arm| arm.result.clone()));
    let result_type = infer_common_scalar_expr_type_with_ctes(
        &result_exprs,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
        "CASE result expressions with a common type",
    )?;

    let (bound_arg, arg_type) = if let Some(arg) = arg {
        (
            Some(bind_expr_with_outer_and_ctes(
                arg,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?),
            Some(infer_sql_expr_type_with_ctes(
                arg,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )),
        )
    } else {
        (None, None)
    };

    let mut bound_arms = Vec::with_capacity(args.len());
    for arm in args {
        let condition = if let Some(arg_type) = arg_type {
            bind_bound_comparison_expr(
                "=",
                OpExprKind::Eq,
                Expr::CaseTest(Box::new(BoundCaseTestExpr { type_id: arg_type })),
                arg_type,
                arg_type,
                &arm.expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?
        } else {
            let expr_type = infer_sql_expr_type_with_ctes(
                &arm.expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if expr_type != SqlType::new(SqlTypeKind::Bool) {
                return Err(ParseError::UnexpectedToken {
                    expected: "boolean CASE condition",
                    actual: "CASE WHEN expression must return boolean".into(),
                });
            }
            bind_expr_with_outer_and_ctes(
                &arm.expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?
        };
        let raw_result_type = infer_sql_expr_type_with_ctes(
            &arm.result,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
        let bound_result = bind_expr_with_outer_and_ctes(
            &arm.result,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?;
        bound_arms.push(BoundCaseWhen {
            expr: condition,
            result: coerce_bound_expr(bound_result, raw_result_type, result_type),
        });
    }

    let raw_default_type = infer_sql_expr_type_with_ctes(
        default_expr,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    );
    let bound_default = bind_expr_with_outer_and_ctes(
        default_expr,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;

    Ok(Expr::Case(Box::new(BoundCaseExpr {
        casetype: result_type,
        arg: bound_arg.map(Box::new),
        args: bound_arms,
        defresult: Box::new(coerce_bound_expr(
            bound_default,
            raw_default_type,
            result_type,
        )),
    })))
}

fn bind_coalesce_call(
    args: &[SqlFunctionArg],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if args.iter().any(|arg| arg.name.is_some()) {
        return Err(ParseError::UnexpectedToken {
            expected: "positional COALESCE arguments",
            actual: "COALESCE with named arguments".into(),
        });
    }
    if args.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "at least one COALESCE argument",
            actual: format!("COALESCE({} args)", args.len()),
        });
    }
    let lowered_args = args.iter().map(|arg| arg.value.clone()).collect::<Vec<_>>();
    let common_type = infer_common_scalar_expr_type_with_ctes(
        &lowered_args,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
        "COALESCE arguments with a common type",
    )?;
    let mut bound_args = Vec::with_capacity(lowered_args.len());
    for arg in &lowered_args {
        let arg_type =
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes);
        let bound =
            bind_expr_with_outer_and_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes)?;
        bound_args.push(coerce_bound_expr(bound, arg_type, common_type));
    }
    let mut iter = bound_args.into_iter().rev();
    let mut expr = iter.next().expect("coalesce arity validated");
    for arg in iter {
        expr = Expr::Coalesce(Box::new(arg), Box::new(expr));
    }
    Ok(expr)
}

fn bind_nullif_call(
    args: &[SqlFunctionArg],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if args.iter().any(|arg| arg.name.is_some()) {
        return Err(ParseError::UnexpectedToken {
            expected: "positional NULLIF arguments",
            actual: "NULLIF with named arguments".into(),
        });
    }
    if args.len() != 2 {
        return Err(ParseError::UnexpectedToken {
            expected: "exactly two NULLIF arguments",
            actual: format!("NULLIF({} args)", args.len()),
        });
    }

    let lowered_args = args.iter().map(|arg| arg.value.clone()).collect::<Vec<_>>();
    let common_type = infer_common_scalar_expr_type_with_ctes(
        &lowered_args,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
        "NULLIF arguments with a common type",
    )?;

    let left_type = infer_sql_expr_type_with_ctes(
        &args[0].value,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    );
    let right_type = infer_sql_expr_type_with_ctes(
        &args[1].value,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    );
    let left = coerce_bound_expr(
        bind_expr_with_outer_and_ctes(
            &args[0].value,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        left_type,
        common_type,
    );
    let right = coerce_bound_expr(
        bind_expr_with_outer_and_ctes(
            &args[1].value,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
        right_type,
        common_type,
    );

    Ok(Expr::Case(Box::new(BoundCaseExpr {
        casetype: common_type,
        arg: None,
        args: vec![BoundCaseWhen {
            expr: Expr::op_auto(OpExprKind::Eq, vec![left.clone(), right]),
            result: Expr::Cast(Box::new(Expr::Const(Value::Null)), common_type),
        }],
        defresult: Box::new(left),
    })))
}

fn validate_catalog_backed_explicit_cast(
    source_type: SqlType,
    target_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    if source_type.element_type() == target_type.element_type() {
        return Ok(());
    }
    if source_type.is_array || !is_text_like_type(source_type) {
        return Ok(());
    }
    if target_type.is_array {
        return Ok(());
    }
    if explicit_text_input_cast_exists(catalog, target_type) {
        return Ok(());
    }
    Err(ParseError::UnexpectedToken {
        expected: "supported explicit cast",
        actual: format!(
            "cannot cast type {} to {}",
            sql_type_name(source_type),
            sql_type_name(target_type)
        ),
    })
}

fn bind_regprocedure_literal_cast(
    expr: &SqlExpr,
    target_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    let Some(signature) = regprocedure_literal_text(expr) else {
        return Ok(None);
    };
    let proc_oid = resolve_regprocedure_signature(signature, catalog)?;
    Ok(Some(Expr::Cast(
        Box::new(Expr::Const(Value::Int64(proc_oid as i64))),
        target_type,
    )))
}

fn bind_regrole_literal_cast(
    expr: &SqlExpr,
    target_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    let Some(role_name) = regrole_literal_text(expr) else {
        return Ok(None);
    };
    let Some(visible_catalog) = catalog.materialize_visible_catalog() else {
        return Ok(None);
    };
    let authid_rows = visible_catalog.authid_rows();
    let role =
        find_role_by_name(&authid_rows, role_name).ok_or_else(|| ParseError::UnexpectedToken {
            expected: "existing role name",
            actual: role_name.to_string(),
        })?;
    Ok(Some(Expr::Cast(
        Box::new(Expr::Const(Value::Int64(role.oid as i64))),
        target_type,
    )))
}

fn regrole_literal_text(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Const(Value::Text(text)) => Some(text.as_str()),
        SqlExpr::Const(Value::TextRef(_, _)) => None,
        _ => None,
    }
}

fn regprocedure_literal_text(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Const(Value::Text(text)) => Some(text.as_str()),
        SqlExpr::Const(Value::TextRef(_, _)) => None,
        _ => None,
    }
}

fn bind_xml_expr(
    xml: &crate::include::nodes::parsenodes::RawXmlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let text_type = SqlType::new(SqlTypeKind::Text);
    let xml_type = SqlType::new(SqlTypeKind::Xml);
    let bind_child = |expr: &SqlExpr| {
        bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)
    };
    let bind_as = |expr: &SqlExpr, target: SqlType| -> Result<Expr, ParseError> {
        let source =
            infer_sql_expr_type_with_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes);
        Ok(coerce_bound_expr(bind_child(expr)?, source, target))
    };

    let mut name = xml.name.clone();
    let mut named_args = Vec::new();
    let mut arg_names = xml.arg_names.clone();
    let mut args = Vec::new();
    let mut target_type = None;

    match xml.op {
        crate::include::nodes::parsenodes::RawXmlExprOp::Parse => {
            args = xml
                .args
                .iter()
                .map(|arg| bind_as(arg, text_type))
                .collect::<Result<Vec<_>, _>>()?;
        }
        crate::include::nodes::parsenodes::RawXmlExprOp::Serialize => {
            args = xml
                .args
                .iter()
                .map(|arg| bind_as(arg, xml_type))
                .collect::<Result<Vec<_>, _>>()?;
            let resolved = resolve_raw_type_name(
                &xml.target_type.clone().ok_or(ParseError::UnexpectedEof)?,
                catalog,
            )?;
            if resolved.is_array
                || !matches!(
                    resolved.kind,
                    SqlTypeKind::Text | SqlTypeKind::Varchar | SqlTypeKind::Char
                )
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "text, character, or character varying",
                    actual: sql_type_name(resolved),
                });
            }
            target_type = Some(resolved);
        }
        crate::include::nodes::parsenodes::RawXmlExprOp::Root => {
            if let Some(first) = xml.args.first() {
                args.push(bind_as(first, xml_type)?);
            }
            if let Some(version) = xml.args.get(1) {
                args.push(bind_as(version, text_type)?);
            }
        }
        crate::include::nodes::parsenodes::RawXmlExprOp::Pi => {
            args = xml
                .args
                .iter()
                .map(|arg| bind_as(arg, text_type))
                .collect::<Result<Vec<_>, _>>()?;
        }
        crate::include::nodes::parsenodes::RawXmlExprOp::IsDocument => {
            args = xml
                .args
                .iter()
                .map(|arg| bind_as(arg, xml_type))
                .collect::<Result<Vec<_>, _>>()?;
        }
        crate::include::nodes::parsenodes::RawXmlExprOp::Element => {
            let mut seen_names = BTreeSet::new();
            for (raw_expr, raw_name) in xml.named_args.iter().zip(xml.arg_names.iter()) {
                let inferred_name = if raw_name.is_empty() {
                    match raw_expr {
                        SqlExpr::Column(column)
                            if !column.contains('.') && !column.ends_with(".*") =>
                        {
                            column.clone()
                        }
                        _ => {
                            return Err(ParseError::UnexpectedToken {
                                expected: "attribute alias for non-column XMLATTRIBUTES expression",
                                actual: "XMLATTRIBUTES expression".into(),
                            });
                        }
                    }
                } else {
                    raw_name.clone()
                };
                if !seen_names.insert(inferred_name.clone()) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "distinct XML attribute names",
                        actual: inferred_name,
                    });
                }
                named_args.push(bind_child(raw_expr)?);
                arg_names.push(inferred_name);
            }
            args = xml
                .args
                .iter()
                .map(bind_child)
                .collect::<Result<Vec<_>, _>>()?;
        }
        crate::include::nodes::parsenodes::RawXmlExprOp::Forest => {
            arg_names.clear();
            for (raw_expr, raw_name) in xml.args.iter().zip(xml.arg_names.iter()) {
                let inferred_name = if raw_name.is_empty() {
                    match raw_expr {
                        SqlExpr::Column(column)
                            if !column.contains('.') && !column.ends_with(".*") =>
                        {
                            column.clone()
                        }
                        _ => {
                            return Err(ParseError::UnexpectedToken {
                                expected: "element alias for non-column XMLFOREST expression",
                                actual: "XMLFOREST expression".into(),
                            });
                        }
                    }
                } else {
                    raw_name.clone()
                };
                arg_names.push(inferred_name);
                args.push(bind_child(raw_expr)?);
            }
        }
        crate::include::nodes::parsenodes::RawXmlExprOp::Concat => {
            args = xml
                .args
                .iter()
                .map(|arg| bind_as(arg, xml_type))
                .collect::<Result<Vec<_>, _>>()?;
        }
    }

    Ok(Expr::Xml(Box::new(
        crate::include::nodes::primnodes::XmlExpr {
            op: match xml.op {
                crate::include::nodes::parsenodes::RawXmlExprOp::Concat => {
                    crate::include::nodes::primnodes::XmlExprOp::Concat
                }
                crate::include::nodes::parsenodes::RawXmlExprOp::Element => {
                    crate::include::nodes::primnodes::XmlExprOp::Element
                }
                crate::include::nodes::parsenodes::RawXmlExprOp::Forest => {
                    crate::include::nodes::primnodes::XmlExprOp::Forest
                }
                crate::include::nodes::parsenodes::RawXmlExprOp::Parse => {
                    crate::include::nodes::primnodes::XmlExprOp::Parse
                }
                crate::include::nodes::parsenodes::RawXmlExprOp::Pi => {
                    crate::include::nodes::primnodes::XmlExprOp::Pi
                }
                crate::include::nodes::parsenodes::RawXmlExprOp::Root => {
                    crate::include::nodes::primnodes::XmlExprOp::Root
                }
                crate::include::nodes::parsenodes::RawXmlExprOp::Serialize => {
                    crate::include::nodes::primnodes::XmlExprOp::Serialize
                }
                crate::include::nodes::parsenodes::RawXmlExprOp::IsDocument => {
                    crate::include::nodes::primnodes::XmlExprOp::IsDocument
                }
            },
            name: name.take(),
            named_args,
            arg_names,
            args,
            xml_option: xml.xml_option,
            indent: xml.indent,
            target_type,
            standalone: xml.standalone,
        },
    )))
}

fn resolve_regprocedure_signature(
    signature: &str,
    catalog: &dyn CatalogLookup,
) -> Result<u32, ParseError> {
    let Some(open_paren) = signature.rfind('(') else {
        return Err(ParseError::UnexpectedToken {
            expected: "function signature",
            actual: signature.to_string(),
        });
    };
    let Some(arg_sql) = signature.get(open_paren + 1..signature.len().saturating_sub(1)) else {
        return Err(ParseError::UnexpectedToken {
            expected: "function signature",
            actual: signature.to_string(),
        });
    };
    if !signature.ends_with(')') {
        return Err(ParseError::UnexpectedToken {
            expected: "function signature",
            actual: signature.to_string(),
        });
    }
    let proc_name = signature[..open_paren].trim();
    if proc_name.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "function name",
            actual: signature.to_string(),
        });
    }
    let arg_type_oids = if arg_sql.trim().is_empty() {
        Vec::new()
    } else {
        arg_sql
            .split(',')
            .map(|arg| {
                let raw_type = crate::backend::parser::parse_type_name(arg.trim())?;
                let sql_type = resolve_raw_type_name(&raw_type, catalog)?;
                catalog
                    .type_oid_for_sql_type(sql_type)
                    .ok_or_else(|| ParseError::UnsupportedType(sql_type_name(sql_type)))
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let normalized_name = normalize_catalog_lookup_name(proc_name);
    let matches = catalog
        .proc_rows_by_name(normalized_name)
        .into_iter()
        .filter(|row| parse_proc_argtype_oids(&row.proargtypes) == Some(arg_type_oids.clone()))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [row] => Ok(row.oid),
        [] => Err(ParseError::UnexpectedToken {
            expected: "existing function signature",
            actual: signature.to_string(),
        }),
        _ => Err(ParseError::UnexpectedToken {
            expected: "unambiguous function signature",
            actual: signature.to_string(),
        }),
    }
}

fn parse_proc_argtype_oids(argtypes: &str) -> Option<Vec<u32>> {
    if argtypes.trim().is_empty() {
        return Some(Vec::new());
    }
    argtypes
        .split_whitespace()
        .map(|oid| oid.parse::<u32>().ok())
        .collect()
}
