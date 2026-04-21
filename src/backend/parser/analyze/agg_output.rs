use super::agg_output_special::*;
use super::expr::raise_expr_varlevels;
use super::*;
use crate::include::nodes::primnodes::{OpExprKind, WindowFuncKind};
use std::cell::RefCell;
use std::collections::HashMap;

#[derive(Debug, Clone)]
struct GroupedAggCteContext {
    visible_ctes: Vec<BoundCte>,
    local_ctes: HashMap<usize, String>,
}

thread_local! {
    static GROUPED_AGG_CTE_CONTEXT: RefCell<Vec<GroupedAggCteContext>> = const { RefCell::new(Vec::new()) };
}

pub(super) fn with_grouped_agg_cte_context<T>(
    visible_ctes: &[BoundCte],
    local_ctes: &[BoundCte],
    f: impl FnOnce() -> Result<T, ParseError>,
) -> Result<T, ParseError> {
    let context = GroupedAggCteContext {
        visible_ctes: visible_ctes.to_vec(),
        local_ctes: local_ctes
            .iter()
            .map(|cte| (cte.cte_id, cte.name.clone()))
            .collect(),
    };
    GROUPED_AGG_CTE_CONTEXT.with(|stack| stack.borrow_mut().push(context));
    let result = f();
    GROUPED_AGG_CTE_CONTEXT.with(|stack| {
        let popped = stack.borrow_mut().pop();
        debug_assert!(
            popped.is_some(),
            "grouped aggregate CTE context stack underflow"
        );
    });
    result
}

fn current_grouped_agg_cte_context() -> Option<GroupedAggCteContext> {
    GROUPED_AGG_CTE_CONTEXT.with(|stack| stack.borrow().last().cloned())
}

pub(super) fn current_grouped_agg_visible_ctes() -> Vec<BoundCte> {
    current_grouped_agg_cte_context()
        .map(|ctx| ctx.visible_ctes)
        .unwrap_or_default()
}

pub(super) fn bind_grouped_plain_expr(
    expr: &SqlExpr,
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Expr, ParseError> {
    let visible_ctes = current_grouped_agg_visible_ctes();
    bind_expr_with_outer_and_ctes(
        expr,
        input_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        &visible_ctes,
    )
}

pub(super) fn grouped_infer_sql_expr_type(
    expr: &SqlExpr,
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> SqlType {
    let visible_ctes = current_grouped_agg_visible_ctes();
    infer_sql_expr_type_with_ctes(
        expr,
        input_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        &visible_ctes,
    )
}

pub(super) fn grouped_infer_common_scalar_expr_type(
    exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    description: &'static str,
) -> Result<SqlType, ParseError> {
    let visible_ctes = current_grouped_agg_visible_ctes();
    infer_common_scalar_expr_type_with_ctes(
        exprs,
        input_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        &visible_ctes,
        description,
    )
}

pub(super) fn reject_nested_local_ctes_in_agg_expr(expr: &Expr) -> Result<(), ParseError> {
    let Some(context) = current_grouped_agg_cte_context() else {
        return Ok(());
    };
    if let Some(cte_name) = expr_references_local_cte(expr, &context.local_ctes) {
        return Err(ParseError::OuterLevelAggregateNestedCte(cte_name));
    }
    Ok(())
}

fn expr_references_local_cte(expr: &Expr, local_ctes: &HashMap<usize, String>) -> Option<String> {
    match expr {
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => None,
        Expr::Aggref(agg) => agg
            .args
            .iter()
            .find_map(|arg| expr_references_local_cte(arg, local_ctes))
            .or_else(|| {
                agg.aggfilter
                    .as_ref()
                    .and_then(|expr| expr_references_local_cte(expr, local_ctes))
            }),
        Expr::WindowFunc(window) => window
            .args
            .iter()
            .find_map(|arg| expr_references_local_cte(arg, local_ctes)),
        Expr::Op(op) => op
            .args
            .iter()
            .find_map(|arg| expr_references_local_cte(arg, local_ctes)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .find_map(|arg| expr_references_local_cte(arg, local_ctes)),
        Expr::Case(case_expr) => case_expr
            .arg
            .as_deref()
            .and_then(|arg| expr_references_local_cte(arg, local_ctes))
            .or_else(|| {
                case_expr.args.iter().find_map(|arm| {
                    expr_references_local_cte(&arm.expr, local_ctes)
                        .or_else(|| expr_references_local_cte(&arm.result, local_ctes))
                })
            })
            .or_else(|| expr_references_local_cte(&case_expr.defresult, local_ctes)),
        Expr::Func(func) => func
            .args
            .iter()
            .find_map(|arg| expr_references_local_cte(arg, local_ctes)),
        Expr::SubLink(sublink) => sublink
            .testexpr
            .as_deref()
            .and_then(|expr| expr_references_local_cte(expr, local_ctes))
            .or_else(|| query_references_local_cte(&sublink.subselect, local_ctes)),
        Expr::SubPlan(subplan) => subplan
            .testexpr
            .as_deref()
            .and_then(|expr| expr_references_local_cte(expr, local_ctes))
            .or_else(|| {
                subplan
                    .args
                    .iter()
                    .find_map(|arg| expr_references_local_cte(arg, local_ctes))
            }),
        Expr::ScalarArrayOp(saop) => expr_references_local_cte(&saop.left, local_ctes)
            .or_else(|| expr_references_local_cte(&saop.right, local_ctes)),
        Expr::Cast(inner, _) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            expr_references_local_cte(inner, local_ctes)
        }
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
        } => expr_references_local_cte(expr, local_ctes)
            .or_else(|| expr_references_local_cte(pattern, local_ctes))
            .or_else(|| {
                escape
                    .as_deref()
                    .and_then(|expr| expr_references_local_cte(expr, local_ctes))
            }),
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => expr_references_local_cte(left, local_ctes)
            .or_else(|| expr_references_local_cte(right, local_ctes)),
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .find_map(|expr| expr_references_local_cte(expr, local_ctes)),
        Expr::Row { fields, .. } => fields
            .iter()
            .find_map(|(_, expr)| expr_references_local_cte(expr, local_ctes)),
        Expr::FieldSelect { expr, .. } => expr_references_local_cte(expr, local_ctes),
        Expr::ArraySubscript { array, subscripts } => expr_references_local_cte(array, local_ctes)
            .or_else(|| {
                subscripts.iter().find_map(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .and_then(|expr| expr_references_local_cte(expr, local_ctes))
                        .or_else(|| {
                            subscript
                                .upper
                                .as_ref()
                                .and_then(|expr| expr_references_local_cte(expr, local_ctes))
                        })
                })
            }),
        Expr::Xml(xml) => xml
            .child_exprs()
            .find_map(|child| expr_references_local_cte(child, local_ctes)),
    }
}

fn query_references_local_cte(
    query: &Query,
    local_ctes: &HashMap<usize, String>,
) -> Option<String> {
    for rte in &query.rtable {
        match &rte.kind {
            RangeTblEntryKind::Cte { cte_id, query } => {
                if let Some(name) = local_ctes.get(cte_id) {
                    return Some(name.clone());
                }
                if let Some(name) = query_references_local_cte(query, local_ctes) {
                    return Some(name);
                }
            }
            RangeTblEntryKind::Subquery { query } => {
                if let Some(name) = query_references_local_cte(query, local_ctes) {
                    return Some(name);
                }
            }
            RangeTblEntryKind::Join { joinaliasvars, .. } => {
                if let Some(name) = joinaliasvars
                    .iter()
                    .find_map(|expr| expr_references_local_cte(expr, local_ctes))
                {
                    return Some(name);
                }
            }
            RangeTblEntryKind::Values { rows, .. } => {
                if let Some(name) = rows
                    .iter()
                    .flatten()
                    .find_map(|expr| expr_references_local_cte(expr, local_ctes))
                {
                    return Some(name);
                }
            }
            RangeTblEntryKind::Function { call } => {
                let args = match call {
                    SetReturningCall::GenerateSeries {
                        start, stop, step, ..
                    } => vec![start, stop, step],
                    SetReturningCall::Unnest { args, .. }
                    | SetReturningCall::JsonTableFunction { args, .. }
                    | SetReturningCall::JsonRecordFunction { args, .. }
                    | SetReturningCall::RegexTableFunction { args, .. }
                    | SetReturningCall::TextSearchTableFunction { args, .. }
                    | SetReturningCall::UserDefined { args, .. } => args.iter().collect(),
                };
                if let Some(name) = args
                    .into_iter()
                    .find_map(|expr| expr_references_local_cte(expr, local_ctes))
                {
                    return Some(name);
                }
            }
            RangeTblEntryKind::Result
            | RangeTblEntryKind::Relation { .. }
            | RangeTblEntryKind::WorkTable { .. } => {}
        }
    }
    if let Some(name) = query
        .target_list
        .iter()
        .find_map(|target| expr_references_local_cte(&target.expr, local_ctes))
    {
        return Some(name);
    }
    if let Some(name) = query
        .where_qual
        .as_ref()
        .and_then(|expr| expr_references_local_cte(expr, local_ctes))
    {
        return Some(name);
    }
    if let Some(name) = query
        .group_by
        .iter()
        .find_map(|expr| expr_references_local_cte(expr, local_ctes))
    {
        return Some(name);
    }
    if let Some(name) = query.accumulators.iter().find_map(|accum| {
        accum
            .args
            .iter()
            .find_map(|expr| expr_references_local_cte(expr, local_ctes))
            .or_else(|| {
                accum
                    .filter
                    .as_ref()
                    .and_then(|expr| expr_references_local_cte(expr, local_ctes))
            })
    }) {
        return Some(name);
    }
    if let Some(name) = query.window_clauses.iter().find_map(|clause| {
        clause
            .functions
            .iter()
            .find_map(|func| {
                func.args
                    .iter()
                    .find_map(|expr| expr_references_local_cte(expr, local_ctes))
            })
            .or_else(|| {
                clause
                    .spec
                    .partition_by
                    .iter()
                    .find_map(|expr| expr_references_local_cte(expr, local_ctes))
            })
            .or_else(|| {
                clause
                    .spec
                    .order_by
                    .iter()
                    .find_map(|item| expr_references_local_cte(&item.expr, local_ctes))
            })
    }) {
        return Some(name);
    }
    if let Some(name) = query
        .having_qual
        .as_ref()
        .and_then(|expr| expr_references_local_cte(expr, local_ctes))
    {
        return Some(name);
    }
    if let Some(name) = query
        .sort_clause
        .iter()
        .find_map(|item| expr_references_local_cte(&item.expr, local_ctes))
    {
        return Some(name);
    }
    if let Some(name) = query.project_set.as_ref().and_then(|targets| {
        targets.iter().find_map(|target| match target {
            ProjectSetTarget::Scalar(target) => expr_references_local_cte(&target.expr, local_ctes),
            ProjectSetTarget::Set { call, .. } => match call {
                SetReturningCall::GenerateSeries {
                    start, stop, step, ..
                } => expr_references_local_cte(start, local_ctes)
                    .or_else(|| expr_references_local_cte(stop, local_ctes))
                    .or_else(|| expr_references_local_cte(step, local_ctes)),
                SetReturningCall::Unnest { args, .. }
                | SetReturningCall::JsonTableFunction { args, .. }
                | SetReturningCall::JsonRecordFunction { args, .. }
                | SetReturningCall::RegexTableFunction { args, .. }
                | SetReturningCall::TextSearchTableFunction { args, .. }
                | SetReturningCall::UserDefined { args, .. } => args
                    .iter()
                    .find_map(|expr| expr_references_local_cte(expr, local_ctes)),
            },
        })
    }) {
        return Some(name);
    }
    if let Some(recursive_union) = &query.recursive_union {
        if let Some(name) = query_references_local_cte(&recursive_union.anchor, local_ctes) {
            return Some(name);
        }
        if let Some(name) = query_references_local_cte(&recursive_union.recursive, local_ctes) {
            return Some(name);
        }
    }
    if let Some(set_operation) = &query.set_operation
        && let Some(name) = set_operation
            .inputs
            .iter()
            .find_map(|input| query_references_local_cte(input, local_ctes))
    {
        return Some(name);
    }
    None
}

fn current_window_state_or_error()
-> Result<std::rc::Rc<std::cell::RefCell<WindowBindingState>>, ParseError> {
    match current_window_state() {
        Some(state) if windows_allowed() => Ok(state),
        Some(_) => Err(nested_window_error()),
        None => Err(window_not_allowed_error()),
    }
}

fn bind_grouped_window_agg_call(
    func: AggFunc,
    args: &[SqlFunctionArg],
    order_by: &[OrderByItem],
    distinct: bool,
    func_variadic: bool,
    filter: Option<&SqlExpr>,
    over: &RawWindowSpec,
    group_by_exprs: &[SqlExpr],
    group_key_exprs: &[Expr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(
        AggFunc,
        Vec<SqlFunctionArg>,
        Vec<OrderByItem>,
        bool,
        bool,
        Option<SqlExpr>,
    )],
    n_keys: usize,
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
            grouped_infer_sql_expr_type(expr, input_scope, catalog, outer_scopes, grouped_outer)
        })
        .collect::<Vec<_>>();
    let resolved = resolve_aggregate_call(catalog, func, &arg_types, func_variadic);
    let bound_args = arg_values
        .iter()
        .map(|expr| {
            with_windows_disallowed(|| {
                bind_agg_output_expr(
                    expr,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    for arg in &bound_args {
        reject_nested_local_ctes_in_agg_expr(arg)?;
    }
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
                bind_agg_output_expr(
                    expr,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )
            })
        })
        .transpose()?;
    let bound_order_by = order_by
        .iter()
        .map(|item| {
            Ok(OrderByEntry {
                expr: bind_agg_output_expr(
                    &item.expr,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
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
        bind_agg_output_expr(
            expr,
            group_by_exprs,
            group_key_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )
    })?;
    let result_type = aggregate_sql_type(func, arg_types.first().copied());
    let kind = WindowFuncKind::Aggregate(crate::include::nodes::primnodes::Aggref {
        aggfnoid: resolved
            .as_ref()
            .map(|call| call.proc_oid)
            .or_else(|| proc_oid_for_builtin_aggregate_function(func))
            .unwrap_or(0),
        aggtype: result_type,
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
        result_type,
    ))
}

fn bind_grouped_window_func_call(
    name: &str,
    args: &[SqlFunctionArg],
    func_variadic: bool,
    over: &RawWindowSpec,
    group_by_exprs: &[SqlExpr],
    group_key_exprs: &[Expr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(
        AggFunc,
        Vec<SqlFunctionArg>,
        Vec<OrderByItem>,
        bool,
        bool,
        Option<SqlExpr>,
    )],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    let state = current_window_state_or_error()?;
    let actual_types = args
        .iter()
        .map(|arg| {
            grouped_infer_sql_expr_type(
                &arg.value,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )
        })
        .collect::<Vec<_>>();
    let mut resolution_types = actual_types.clone();
    if matches!(args.len(), 3)
        && !func_variadic
        && (name.eq_ignore_ascii_case("lag") || name.eq_ignore_ascii_case("lead"))
    {
        let common_type = grouped_infer_common_scalar_expr_type(
            &[args[0].value.clone(), args[2].value.clone()],
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
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
        bind_agg_output_expr(
            expr,
            group_by_exprs,
            group_key_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )
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
                    bind_agg_output_expr(
                        &arg.value,
                        group_by_exprs,
                        group_key_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
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
        return bind_grouped_window_agg_call(
            agg_impl,
            args,
            &[],
            false,
            resolved.func_variadic,
            None,
            over,
            group_by_exprs,
            group_key_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        );
    }
    Err(ParseError::FeatureNotSupported(format!(
        "window function {name}"
    )))
}

fn grouped_key_expr(group_key_exprs: &[Expr], index: usize) -> Expr {
    group_key_exprs.get(index).cloned().unwrap_or_else(|| {
        panic!(
            "grouped aggregate output missing group key expr for position {}; \
                 parser/analyze should provide explicit grouped key identity",
            index + 1
        )
    })
}

pub(super) fn bind_agg_output_expr(
    expr: &SqlExpr,
    group_by_exprs: &[SqlExpr],
    group_key_exprs: &[Expr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(
        AggFunc,
        Vec<SqlFunctionArg>,
        Vec<OrderByItem>,
        bool,
        bool,
        Option<SqlExpr>,
    )],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    bind_agg_output_expr_in_clause(
        expr,
        UngroupedColumnClause::Other,
        group_by_exprs,
        group_key_exprs,
        input_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        agg_list,
        n_keys,
    )
}

pub(super) fn bind_agg_output_expr_in_clause(
    expr: &SqlExpr,
    clause: UngroupedColumnClause,
    group_by_exprs: &[SqlExpr],
    group_key_exprs: &[Expr],
    input_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(
        AggFunc,
        Vec<SqlFunctionArg>,
        Vec<OrderByItem>,
        bool,
        bool,
        Option<SqlExpr>,
    )],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    for (i, gk) in group_by_exprs.iter().enumerate() {
        if gk == expr {
            return Ok(grouped_key_expr(group_key_exprs, i));
        }
    }

    match expr {
        SqlExpr::Xml(_) => Err(ParseError::FeatureNotSupported(
            "xml expressions in grouped aggregate output are not implemented".into(),
        )),
        SqlExpr::Default => Err(ParseError::UnexpectedToken {
            expected: "expression",
            actual: "DEFAULT".into(),
        }),
        SqlExpr::Row(_) => Err(ParseError::UnexpectedToken {
            expected: "implemented row expression",
            actual: "ROW(...)".into(),
        }),
        SqlExpr::AggCall {
            func,
            args,
            order_by,
            distinct,
            func_variadic,
            filter,
            over,
        } => {
            if let Some(raw_over) = over {
                return bind_grouped_window_agg_call(
                    *func,
                    args,
                    order_by,
                    *distinct,
                    *func_variadic,
                    filter.as_deref(),
                    raw_over,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                );
            }
            let entry = (
                *func,
                args.clone(),
                order_by.clone(),
                *distinct,
                *func_variadic,
                filter.as_deref().cloned(),
            );
            for (i, agg) in agg_list.iter().enumerate() {
                if *agg == entry {
                    let arg_values: Vec<SqlExpr> =
                        args.iter().map(|arg| arg.value.clone()).collect();
                    let arg_types = arg_values
                        .iter()
                        .map(|e| {
                            grouped_infer_sql_expr_type(
                                e,
                                input_scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                            )
                        })
                        .collect::<Vec<_>>();
                    let resolved =
                        resolve_aggregate_call(catalog, *func, &arg_types, *func_variadic);
                    let aggfnoid = resolved
                        .as_ref()
                        .map(|call| call.proc_oid)
                        .or_else(|| proc_oid_for_builtin_aggregate_function(*func))
                        .unwrap_or(0);
                    let agg_variadic = resolved
                        .as_ref()
                        .map(|call| call.func_variadic)
                        .unwrap_or(*func_variadic);
                    let bound_args = arg_values
                        .iter()
                        .map(|arg| {
                            bind_grouped_plain_expr(
                                arg,
                                input_scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    for arg in &bound_args {
                        reject_nested_local_ctes_in_agg_expr(arg)?;
                    }
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
                        .as_deref()
                        .map(|expr| {
                            bind_grouped_plain_expr(
                                expr,
                                input_scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                            )
                        })
                        .transpose()?;
                    if let Some(filter) = &bound_filter {
                        reject_nested_local_ctes_in_agg_expr(filter)?;
                    }
                    let bound_order_by = order_by
                        .iter()
                        .map(|item| {
                            Ok(OrderByEntry {
                                expr: bind_grouped_plain_expr(
                                    &item.expr,
                                    input_scope,
                                    catalog,
                                    outer_scopes,
                                    grouped_outer,
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
                    return Ok(Expr::aggref(
                        aggfnoid,
                        aggregate_sql_type(*func, arg_types.first().copied()),
                        agg_variadic,
                        *distinct,
                        coerced_args,
                        bound_order_by,
                        bound_filter,
                        i,
                    ));
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
                        return outer_scopes
                            .get(depth)
                            .and_then(|scope| scope.output_exprs.get(index))
                            .cloned()
                            .map(|expr| raise_expr_varlevels(expr, depth + 1))
                            .ok_or_else(|| ParseError::UnexpectedToken {
                                expected: "resolved outer grouped column",
                                actual: name.clone(),
                            });
                    }
                };
            for (i, gk) in group_by_exprs.iter().enumerate() {
                if let SqlExpr::Column(gk_name) = gk
                    && let Ok(gk_index) = resolve_column(input_scope, gk_name)
                    && gk_index == col_index
                {
                    return Ok(grouped_key_expr(group_key_exprs, i));
                }
            }
            Err(build_ungrouped_column_error(
                input_scope,
                col_index,
                name,
                clause,
            ))
        }
        SqlExpr::Const(v) => Ok(Expr::Const(v.clone())),
        SqlExpr::IntegerLiteral(value) => Ok(Expr::Const(bind_integer_literal(value)?)),
        SqlExpr::NumericLiteral(value) => Ok(Expr::Const(bind_numeric_literal(value)?)),
        SqlExpr::BinaryOperator { op, .. } => Err(ParseError::UnexpectedToken {
            expected: "grouped expression",
            actual: format!("unsupported operator {op}"),
        }),
        SqlExpr::Add(l, r) => Ok(Expr::op_auto(
            OpExprKind::Add,
            vec![
                bind_agg_output_expr_in_clause(
                    l,
                    clause.clone(),
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr_in_clause(
                    r,
                    clause,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::Sub(l, r) => Ok(Expr::op_auto(
            OpExprKind::Sub,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::BitAnd(l, r) => Ok(Expr::op_auto(
            OpExprKind::BitAnd,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::BitOr(l, r) => Ok(Expr::op_auto(
            OpExprKind::BitOr,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::BitXor(l, r) => Ok(Expr::op_auto(
            OpExprKind::BitXor,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::Shl(l, r) => Ok(Expr::op_auto(
            OpExprKind::Shl,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::Shr(l, r) => Ok(Expr::op_auto(
            OpExprKind::Shr,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::Mul(l, r) => Ok(Expr::op_auto(
            OpExprKind::Mul,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::Div(l, r) => Ok(Expr::op_auto(
            OpExprKind::Div,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::Mod(l, r) => Ok(Expr::op_auto(
            OpExprKind::Mod,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::Concat(l, r) => bind_grouped_concat_expr(
            l,
            r,
            group_by_exprs,
            group_key_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        ),
        SqlExpr::UnaryPlus(inner) => Ok(Expr::op_auto(
            OpExprKind::UnaryPlus,
            vec![bind_agg_output_expr(
                inner,
                group_by_exprs,
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?],
        )),
        SqlExpr::Negate(inner) => Ok(Expr::op_auto(
            OpExprKind::Negate,
            vec![bind_agg_output_expr(
                inner,
                group_by_exprs,
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?],
        )),
        SqlExpr::BitNot(inner) => Ok(Expr::op_auto(
            OpExprKind::BitNot,
            vec![bind_agg_output_expr(
                inner,
                group_by_exprs,
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?],
        )),
        SqlExpr::Cast(inner, ty) => {
            let bound_inner = if let SqlExpr::ArrayLiteral(elements) = inner.as_ref() {
                Expr::ArrayLiteral {
                    elements: elements
                        .iter()
                        .map(|element| {
                            bind_agg_output_expr(
                                element,
                                group_by_exprs,
                                group_key_exprs,
                                input_scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                agg_list,
                                n_keys,
                            )
                        })
                        .collect::<Result<_, _>>()?,
                    array_type: raw_type_name_hint(ty),
                }
            } else {
                bind_agg_output_expr(
                    inner,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?
            };
            Ok(Expr::Cast(Box::new(bound_inner), raw_type_name_hint(ty)))
        }
        SqlExpr::Eq(l, r) => Ok(Expr::op_auto(
            OpExprKind::Eq,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::NotEq(l, r) => Ok(Expr::op_auto(
            OpExprKind::NotEq,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::Lt(l, r) => Ok(Expr::op_auto(
            OpExprKind::Lt,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::LtEq(l, r) => Ok(Expr::op_auto(
            OpExprKind::LtEq,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::Gt(l, r) => Ok(Expr::op_auto(
            OpExprKind::Gt,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::GtEq(l, r) => Ok(Expr::op_auto(
            OpExprKind::GtEq,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::RegexMatch(l, r) => Ok(Expr::op_auto(
            OpExprKind::RegexMatch,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Ok(Expr::Like {
            expr: Box::new(bind_agg_output_expr(
                expr,
                group_by_exprs,
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            pattern: Box::new(bind_agg_output_expr(
                pattern,
                group_by_exprs,
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            escape: match escape {
                Some(value) => Some(Box::new(bind_agg_output_expr(
                    value,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?)),
                None => None,
            },
            case_insensitive: *case_insensitive,
            negated: *negated,
        }),
        SqlExpr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Ok(Expr::Similar {
            expr: Box::new(bind_agg_output_expr(
                expr,
                group_by_exprs,
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            pattern: Box::new(bind_agg_output_expr(
                pattern,
                group_by_exprs,
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            escape: match escape {
                Some(value) => Some(Box::new(bind_agg_output_expr(
                    value,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?)),
                None => None,
            },
            negated: *negated,
        }),
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            let default_sql_expr = SqlExpr::Const(Value::Null);
            let default_expr = defresult.as_deref().unwrap_or(&default_sql_expr);
            let mut result_exprs = Vec::with_capacity(args.len() + 1);
            result_exprs.push(default_expr.clone());
            result_exprs.extend(args.iter().map(|arm| arm.result.clone()));
            let result_type = grouped_infer_common_scalar_expr_type(
                &result_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                "CASE result expressions with a common type",
            )?;
            let (bound_arg, arg_type) = if let Some(arg) = arg {
                (
                    Some(bind_agg_output_expr(
                        arg,
                        group_by_exprs,
                        group_key_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                    Some(grouped_infer_sql_expr_type(
                        arg,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )),
                )
            } else {
                (None, None)
            };
            let mut bound_arms = Vec::with_capacity(args.len());
            for arm in args {
                let condition = if let Some(arg_type) = arg_type {
                    let right_bound = bind_agg_output_expr(
                        &arm.expr,
                        group_by_exprs,
                        group_key_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?;
                    let raw_right_type = grouped_infer_sql_expr_type(
                        &arm.expr,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    );
                    bind_lowered_comparison_expr(
                        "=",
                        OpExprKind::Eq,
                        Expr::CaseTest(Box::new(crate::include::nodes::primnodes::CaseTestExpr {
                            type_id: arg_type,
                        })),
                        arg_type,
                        arg_type,
                        right_bound,
                        raw_right_type,
                        raw_right_type,
                        catalog,
                    )?
                } else {
                    let expr_type = grouped_infer_sql_expr_type(
                        &arm.expr,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    );
                    if expr_type != SqlType::new(SqlTypeKind::Bool) {
                        return Err(ParseError::UnexpectedToken {
                            expected: "boolean CASE condition",
                            actual: "CASE WHEN expression must return boolean".into(),
                        });
                    }
                    bind_agg_output_expr(
                        &arm.expr,
                        group_by_exprs,
                        group_key_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?
                };
                let raw_result_type = grouped_infer_sql_expr_type(
                    &arm.result,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                );
                let bound_result = bind_agg_output_expr(
                    &arm.result,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?;
                bound_arms.push(crate::include::nodes::primnodes::CaseWhen {
                    expr: condition,
                    result: coerce_bound_expr(bound_result, raw_result_type, result_type),
                });
            }
            let raw_default_type = grouped_infer_sql_expr_type(
                default_expr,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
            );
            let bound_default = bind_agg_output_expr(
                default_expr,
                group_by_exprs,
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?;
            Ok(Expr::Case(Box::new(
                crate::include::nodes::primnodes::CaseExpr {
                    casetype: result_type,
                    arg: bound_arg.map(Box::new),
                    args: bound_arms,
                    defresult: Box::new(coerce_bound_expr(
                        bound_default,
                        raw_default_type,
                        result_type,
                    )),
                },
            )))
        }
        SqlExpr::And(l, r) => Ok(Expr::and(
            bind_agg_output_expr(
                l,
                group_by_exprs,
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?,
            bind_agg_output_expr(
                r,
                group_by_exprs,
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?,
        )),
        SqlExpr::Or(l, r) => Ok(Expr::or(
            bind_agg_output_expr(
                l,
                group_by_exprs,
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?,
            bind_agg_output_expr(
                r,
                group_by_exprs,
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?,
        )),
        SqlExpr::Not(inner) => Ok(Expr::not(bind_agg_output_expr(
            inner,
            group_by_exprs,
            group_key_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?)),
        SqlExpr::IsNull(inner) => Ok(Expr::IsNull(Box::new(bind_agg_output_expr(
            inner,
            group_by_exprs,
            group_key_exprs,
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
            group_key_exprs,
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
                group_key_exprs,
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
                group_key_exprs,
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
                group_key_exprs,
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
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::ArrayLiteral(elements) => bind_grouped_array_literal(
            elements,
            group_by_exprs,
            group_key_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        ),
        SqlExpr::ArrayOverlap(l, r) => {
            let raw_left_type =
                grouped_infer_sql_expr_type(l, input_scope, catalog, outer_scopes, grouped_outer);
            let raw_right_type =
                grouped_infer_sql_expr_type(r, input_scope, catalog, outer_scopes, grouped_outer);
            let left = bind_agg_output_expr(
                l,
                group_by_exprs,
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?;
            let right = bind_agg_output_expr(
                r,
                group_by_exprs,
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?;
            let right = if matches!(
                &**r,
                SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
            ) {
                if let Expr::ArrayLiteral { array_type, .. } = &left {
                    coerce_bound_expr(right, raw_right_type, *array_type)
                } else {
                    right
                }
            } else {
                right
            };
            let left = if matches!(
                &**l,
                SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
            ) {
                if let Expr::ArrayLiteral { array_type, .. } = &right {
                    coerce_bound_expr(left, raw_left_type, *array_type)
                } else {
                    left
                }
            } else {
                left
            };
            Ok(Expr::op_auto(OpExprKind::ArrayOverlap, vec![left, right]))
        }
        SqlExpr::JsonGet(l, r) => Ok(Expr::op_auto(
            OpExprKind::JsonGet,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::JsonGetText(l, r) => Ok(Expr::op_auto(
            OpExprKind::JsonGetText,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::JsonPath(l, r) => Ok(Expr::op_auto(
            OpExprKind::JsonPath,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::JsonPathText(l, r) => Ok(Expr::op_auto(
            OpExprKind::JsonPathText,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::JsonbContains(l, r) => Ok(Expr::op_auto(
            OpExprKind::JsonbContains,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::JsonbContained(l, r) => Ok(Expr::op_auto(
            OpExprKind::JsonbContained,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::JsonbExists(l, r) => Ok(Expr::op_auto(
            OpExprKind::JsonbExists,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::JsonbExistsAny(l, r) => {
            let left_type =
                infer_sql_expr_type(l, input_scope, catalog, outer_scopes, grouped_outer);
            let right_type =
                infer_sql_expr_type(r, input_scope, catalog, outer_scopes, grouped_outer);
            if is_geometry_type(left_type) || is_geometry_type(right_type) {
                Ok(Expr::builtin_func(
                    BuiltinScalarFunction::GeoIsVertical,
                    None,
                    false,
                    vec![
                        bind_agg_output_expr(
                            l,
                            group_by_exprs,
                            group_key_exprs,
                            input_scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            agg_list,
                            n_keys,
                        )?,
                        bind_agg_output_expr(
                            r,
                            group_by_exprs,
                            group_key_exprs,
                            input_scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            agg_list,
                            n_keys,
                        )?,
                    ],
                ))
            } else {
                Ok(Expr::op_auto(
                    OpExprKind::JsonbExistsAny,
                    vec![
                        bind_agg_output_expr(
                            l,
                            group_by_exprs,
                            group_key_exprs,
                            input_scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            agg_list,
                            n_keys,
                        )?,
                        bind_agg_output_expr(
                            r,
                            group_by_exprs,
                            group_key_exprs,
                            input_scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            agg_list,
                            n_keys,
                        )?,
                    ],
                ))
            }
        }
        SqlExpr::JsonbExistsAll(l, r) => Ok(Expr::op_auto(
            OpExprKind::JsonbExistsAll,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::JsonbPathExists(l, r) => Ok(Expr::op_auto(
            OpExprKind::JsonbPathExists,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::JsonbPathMatch(l, r) => Ok(Expr::op_auto(
            OpExprKind::JsonbPathMatch,
            vec![
                bind_agg_output_expr(
                    l,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    r,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::ScalarSubquery(select) => {
            bind_grouped_scalar_subquery(select, group_by_exprs, input_scope, catalog, outer_scopes)
        }
        SqlExpr::ArraySubquery(select) => {
            bind_grouped_array_subquery(select, group_by_exprs, input_scope, catalog, outer_scopes)
        }
        SqlExpr::Exists(select) => {
            bind_grouped_exists_subquery(select, group_by_exprs, input_scope, catalog, outer_scopes)
        }
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => bind_grouped_in_subquery(
            expr,
            subquery,
            *negated,
            group_by_exprs,
            group_key_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        ),
        SqlExpr::QuantifiedSubquery {
            left,
            op,
            is_all,
            subquery,
        } => bind_grouped_quantified_subquery(
            left,
            *op,
            *is_all,
            subquery,
            group_by_exprs,
            group_key_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        ),
        SqlExpr::QuantifiedArray {
            left,
            op,
            is_all,
            array,
        } => bind_grouped_quantified_array(
            left,
            *op,
            *is_all,
            array,
            group_by_exprs,
            group_key_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        ),
        SqlExpr::ArraySubscript { array, subscripts } => Ok(Expr::ArraySubscript {
            array: Box::new(bind_agg_output_expr(
                array,
                group_by_exprs,
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
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
                                bind_agg_output_expr(
                                    expr,
                                    group_by_exprs,
                                    group_key_exprs,
                                    input_scope,
                                    catalog,
                                    outer_scopes,
                                    grouped_outer,
                                    agg_list,
                                    n_keys,
                                )
                            })
                            .transpose()?,
                        upper: subscript
                            .upper
                            .as_deref()
                            .map(|expr| {
                                bind_agg_output_expr(
                                    expr,
                                    group_by_exprs,
                                    group_key_exprs,
                                    input_scope,
                                    catalog,
                                    outer_scopes,
                                    grouped_outer,
                                    agg_list,
                                    n_keys,
                                )
                            })
                            .transpose()?,
                    })
                })
                .collect::<Result<_, ParseError>>()?,
        }),
        SqlExpr::Random => Ok(Expr::Random),
        SqlExpr::FuncCall {
            name,
            args,
            func_variadic,
            over,
        } => {
            if let Some(raw_over) = over {
                bind_grouped_window_func_call(
                    name,
                    args,
                    *func_variadic,
                    raw_over,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )
            } else {
                bind_grouped_func_call(
                    name,
                    args,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )
            }
        }
        SqlExpr::Subscript { expr, index } => {
            let expr_type =
                infer_sql_expr_type(expr, input_scope, catalog, outer_scopes, grouped_outer);
            if expr_type.element_type().kind != SqlTypeKind::Point
                || expr_type.is_array
                || !(0..=1).contains(index)
            {
                return Err(ParseError::UndefinedOperator {
                    op: "[]",
                    left_type: sql_type_name(expr_type),
                    right_type: "integer".into(),
                });
            }
            Ok(Expr::builtin_func(
                if *index == 0 {
                    BuiltinScalarFunction::GeoPointX
                } else {
                    BuiltinScalarFunction::GeoPointY
                },
                None,
                false,
                vec![bind_agg_output_expr(
                    expr,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?],
            ))
        }
        SqlExpr::GeometryUnaryOp { op, expr } => Ok(Expr::builtin_func(
            match op {
                GeometryUnaryOp::Center => BuiltinScalarFunction::GeoCenter,
                GeometryUnaryOp::Length => BuiltinScalarFunction::GeoLength,
                GeometryUnaryOp::Npoints => BuiltinScalarFunction::GeoNpoints,
                GeometryUnaryOp::IsVertical => BuiltinScalarFunction::GeoIsVertical,
                GeometryUnaryOp::IsHorizontal => BuiltinScalarFunction::GeoIsHorizontal,
            },
            None,
            false,
            vec![bind_agg_output_expr(
                expr,
                group_by_exprs,
                group_key_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?],
        )),
        SqlExpr::GeometryBinaryOp { op, left, right } => Ok(Expr::builtin_func(
            match op {
                GeometryBinaryOp::Same => BuiltinScalarFunction::GeoSame,
                GeometryBinaryOp::Distance => BuiltinScalarFunction::GeoDistance,
                GeometryBinaryOp::ClosestPoint => BuiltinScalarFunction::GeoClosestPoint,
                GeometryBinaryOp::Intersects => BuiltinScalarFunction::GeoIntersects,
                GeometryBinaryOp::Parallel => BuiltinScalarFunction::GeoParallel,
                GeometryBinaryOp::Perpendicular => BuiltinScalarFunction::GeoPerpendicular,
                GeometryBinaryOp::IsVertical => BuiltinScalarFunction::GeoIsVertical,
                GeometryBinaryOp::IsHorizontal => BuiltinScalarFunction::GeoIsHorizontal,
                GeometryBinaryOp::OverLeft => BuiltinScalarFunction::GeoOverLeft,
                GeometryBinaryOp::OverRight => BuiltinScalarFunction::GeoOverRight,
                GeometryBinaryOp::Below => BuiltinScalarFunction::GeoBelow,
                GeometryBinaryOp::Above => BuiltinScalarFunction::GeoAbove,
                GeometryBinaryOp::OverBelow => BuiltinScalarFunction::GeoOverBelow,
                GeometryBinaryOp::OverAbove => BuiltinScalarFunction::GeoOverAbove,
            },
            None,
            false,
            vec![
                bind_agg_output_expr(
                    left,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
                bind_agg_output_expr(
                    right,
                    group_by_exprs,
                    group_key_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?,
            ],
        )),
        SqlExpr::PrefixOperator { op, .. } => Err(ParseError::UnexpectedToken {
            expected: "grouped expression",
            actual: format!("unsupported operator {op}"),
        }),
        SqlExpr::FieldSelect { field, .. } => Err(ParseError::UnexpectedToken {
            expected: "grouped expression",
            actual: format!("unsupported field selection .{field}"),
        }),
        SqlExpr::CurrentDate => Ok(Expr::CurrentDate),
        SqlExpr::CurrentUser => Ok(Expr::CurrentUser),
        SqlExpr::SessionUser => Ok(Expr::SessionUser),
        SqlExpr::CurrentRole => Ok(Expr::CurrentRole),
        SqlExpr::CurrentTime { precision } => Ok(Expr::CurrentTime {
            precision: *precision,
        }),
        SqlExpr::CurrentTimestamp { precision } => Ok(Expr::CurrentTimestamp {
            precision: *precision,
        }),
        SqlExpr::LocalTime { precision } => Ok(Expr::LocalTime {
            precision: *precision,
        }),
        SqlExpr::LocalTimestamp { precision } => Ok(Expr::LocalTimestamp {
            precision: *precision,
        }),
    }
}

fn build_ungrouped_column_error(
    input_scope: &BoundScope,
    col_index: usize,
    token: &str,
    clause: UngroupedColumnClause,
) -> ParseError {
    let column = &input_scope.columns[col_index];
    let display_name = column
        .relation_names
        .first()
        .map(|relation_name| format!("{relation_name}.{}", column.output_name))
        .unwrap_or_else(|| column.output_name.clone());
    ParseError::UngroupedColumn {
        display_name,
        token: token.to_string(),
        clause,
    }
}
