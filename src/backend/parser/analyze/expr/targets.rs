use super::*;
use crate::backend::utils::record::{
    assign_anonymous_record_descriptor, lookup_anonymous_record_descriptor,
};
pub(crate) enum BoundSelectTargets {
    Plain(Vec<TargetEntry>),
    WithProjectSet {
        project_targets: Vec<ProjectSetTarget>,
        final_targets: Vec<TargetEntry>,
    },
}

#[derive(Clone)]
enum TopLevelSelectSrfTarget {
    Call {
        name: String,
        args: Vec<SqlFunctionArg>,
        func_variadic: bool,
    },
    FieldSelect {
        name: String,
        args: Vec<SqlFunctionArg>,
        func_variadic: bool,
        field: String,
    },
}

struct BoundSelectListSrfTarget {
    call: SetReturningCall,
    sql_type: SqlType,
    column_index: usize,
}

fn input_resno_for_scope_expr(scope: &BoundScope, expr: &Expr) -> Option<usize> {
    scope
        .output_exprs
        .iter()
        .position(|candidate| candidate == expr)
        .map(|index| index + 1)
}

pub(crate) fn bind_select_targets(
    targets: &[SelectItem],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<BoundSelectTargets, ParseError> {
    let mut has_srf = false;
    for item in targets {
        let info = classify_select_target_srf(
            &item.expr,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
        if info.has_nested {
            return Err(ParseError::UnexpectedToken {
                expected: "set-returning function at top level of select list",
                actual: format!("{:?}", item.expr),
            });
        }
        has_srf |= info.top_level.is_some();
    }

    if !has_srf {
        return Ok(BoundSelectTargets::Plain(bind_plain_select_targets(
            targets,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?));
    }

    let mut project_targets = scope
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            ProjectSetTarget::Scalar(
                TargetEntry::new(
                    column.output_name.clone(),
                    scope.output_exprs.get(index).cloned().unwrap_or_else(|| {
                        panic!("bound scope output_exprs missing project-set base column {index}")
                    }),
                    scope.desc.columns[index].sql_type,
                    index + 1,
                )
                .with_input_resno(index + 1),
            )
        })
        .collect::<Vec<_>>();

    let mut final_targets = Vec::new();
    let mut srf_index = 0usize;
    let base_width = scope.columns.len();

    for item in targets {
        if let Some(target) = top_level_set_returning_target(
            &item.expr,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        ) {
            let bound_target = bind_select_list_srf_target(
                &target,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            let output_name = item.output_name.clone();
            project_targets.push(ProjectSetTarget::Set {
                name: output_name.clone(),
                call: bound_target.call,
                sql_type: bound_target.sql_type,
                column_index: bound_target.column_index,
            });
            final_targets.push(
                TargetEntry::new(
                    output_name,
                    Expr::Const(Value::Null),
                    bound_target.sql_type,
                    final_targets.len() + 1,
                )
                .with_input_resno(base_width + srf_index + 1),
            );
            srf_index += 1;
            continue;
        }

        let expr = bind_expr_with_outer_and_ctes(
            &item.expr,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?;
        let input_resno = input_resno_for_scope_expr(scope, &expr);
        final_targets.push(
            TargetEntry::new(
                item.output_name.clone(),
                expr,
                infer_sql_expr_type_with_ctes(
                    &item.expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                ),
                final_targets.len() + 1,
            )
            .with_input_resno_opt(input_resno),
        );
    }

    Ok(BoundSelectTargets::WithProjectSet {
        project_targets,
        final_targets,
    })
}

pub(crate) fn select_targets_contain_set_returning_call(
    targets: &[SelectItem],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> bool {
    targets.iter().any(|item| {
        classify_select_target_srf(
            &item.expr,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )
        .top_level
        .is_some()
    })
}

fn bind_plain_select_targets(
    targets: &[SelectItem],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Vec<TargetEntry>, ParseError> {
    let mut entries = Vec::new();
    for item in targets {
        if let SqlExpr::Column(name) = &item.expr {
            if name == "*" {
                entries.extend(expand_star_targets(scope, None)?);
                continue;
            }
            if let Some(relation) = name.strip_suffix(".*") {
                entries.extend(expand_star_targets(scope, Some(relation))?);
                continue;
            }
        }

        let expr = bind_expr_with_outer_and_ctes(
            &item.expr,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?;
        let input_resno = input_resno_for_scope_expr(scope, &expr);
        entries.push(
            TargetEntry::new(
                item.output_name.clone(),
                expr,
                infer_sql_expr_type_with_ctes(
                    &item.expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                ),
                entries.len() + 1,
            )
            .with_input_resno_opt(input_resno),
        );
    }
    Ok(entries)
}

#[derive(Default)]
struct TargetSrfInfo {
    top_level: Option<TopLevelSelectSrfTarget>,
    has_nested: bool,
}

fn classify_select_target_srf(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> TargetSrfInfo {
    if let Some(target) =
        top_level_set_returning_target(expr, scope, catalog, outer_scopes, grouped_outer, ctes)
    {
        TargetSrfInfo {
            top_level: Some(target),
            has_nested: false,
        }
    } else {
        let mut info = TargetSrfInfo::default();
        visit_nested_srfs(
            expr,
            &mut info,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
        info
    }
}

fn top_level_set_returning_target(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Option<TopLevelSelectSrfTarget> {
    match expr {
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            distinct,
            func_variadic,
            filter,
            over,
            ..
        } if func_call_is_set_returning(
            name,
            args.args(),
            *func_variadic,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        ) && order_by.is_empty()
            && !*distinct
            && filter.is_none()
            && over.is_none() =>
        {
            Some(TopLevelSelectSrfTarget::Call {
                name: name.clone(),
                args: args.args().to_vec(),
                func_variadic: *func_variadic,
            })
        }
        SqlExpr::FieldSelect { expr, field } => match expr.as_ref() {
            SqlExpr::FuncCall {
                name,
                args,
                order_by,
                distinct,
                func_variadic,
                filter,
                over,
                ..
            } if func_call_is_set_returning(
                name,
                args.args(),
                *func_variadic,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) && order_by.is_empty()
                && !*distinct
                && filter.is_none()
                && over.is_none() =>
            {
                Some(TopLevelSelectSrfTarget::FieldSelect {
                    name: name.clone(),
                    args: args.args().to_vec(),
                    func_variadic: *func_variadic,
                    field: field.clone(),
                })
            }
            _ => None,
        },
        _ => None,
    }
}

fn bind_select_list_srf_target(
    target: &TopLevelSelectSrfTarget,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<BoundSelectListSrfTarget, ParseError> {
    let (name, args, func_variadic, projected_field) = match target {
        TopLevelSelectSrfTarget::Call {
            name,
            args,
            func_variadic,
        } => (name.as_str(), args.as_slice(), *func_variadic, None),
        TopLevelSelectSrfTarget::FieldSelect {
            name,
            args,
            func_variadic,
            field,
        } => (
            name.as_str(),
            args.as_slice(),
            *func_variadic,
            Some(field.as_str()),
        ),
    };
    let call = bind_select_list_srf_call(
        name,
        args,
        func_variadic,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    let output_columns = call.output_columns();
    let (sql_type, column_index) = match projected_field {
        Some(field) => output_columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name.eq_ignore_ascii_case(field))
            .map(|(index, column)| (column.sql_type, index + 1))
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "record field",
                actual: format!("field selection .{field}"),
            })?,
        None if output_columns.len() == 1 => (output_columns[0].sql_type, 1),
        None => {
            let descriptor = assign_anonymous_record_descriptor(
                output_columns
                    .iter()
                    .map(|column| (column.name.clone(), column.sql_type))
                    .collect(),
            );
            (descriptor.sql_type(), 0)
        }
    };
    Ok(BoundSelectListSrfTarget {
        call,
        sql_type,
        column_index,
    })
}

fn visit_nested_srfs(
    expr: &SqlExpr,
    info: &mut TargetSrfInfo,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) {
    match expr {
        SqlExpr::Collate { expr, .. } => visit_nested_srfs(
            expr,
            info,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        ),
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            distinct,
            func_variadic,
            filter,
            over,
            ..
        } => {
            if func_call_is_set_returning(
                name,
                args.args(),
                *func_variadic,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) && order_by.is_empty()
                && !*distinct
                && filter.is_none()
                && over.is_none()
            {
                info.has_nested = true;
            }
            for arg in args.args() {
                visit_nested_srfs(
                    &arg.value,
                    info,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            for item in order_by {
                visit_nested_srfs(
                    &item.expr,
                    info,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if let Some(filter) = filter.as_deref() {
                visit_nested_srfs(
                    filter,
                    info,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
        }
        SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::BitAnd(left, right)
        | SqlExpr::BitOr(left, right)
        | SqlExpr::BitXor(left, right)
        | SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right)
        | SqlExpr::Concat(left, right)
        | SqlExpr::Eq(left, right)
        | SqlExpr::NotEq(left, right)
        | SqlExpr::Lt(left, right)
        | SqlExpr::LtEq(left, right)
        | SqlExpr::Gt(left, right)
        | SqlExpr::GtEq(left, right)
        | SqlExpr::RegexMatch(left, right)
        | SqlExpr::And(left, right)
        | SqlExpr::Or(left, right)
        | SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right)
        | SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right)
        | SqlExpr::JsonbExists(left, right)
        | SqlExpr::JsonbExistsAny(left, right)
        | SqlExpr::JsonbExistsAll(left, right)
        | SqlExpr::JsonbPathExists(left, right)
        | SqlExpr::JsonbPathMatch(left, right)
        | SqlExpr::JsonGet(left, right)
        | SqlExpr::JsonGetText(left, right)
        | SqlExpr::JsonPath(left, right)
        | SqlExpr::JsonPathText(left, right) => {
            visit_nested_srfs(
                left,
                info,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            visit_nested_srfs(
                right,
                info,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
        }
        SqlExpr::BinaryOperator { left, right, .. } => {
            visit_nested_srfs(
                left,
                info,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            visit_nested_srfs(
                right,
                info,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
        }
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            visit_nested_srfs(
                expr,
                info,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            visit_nested_srfs(
                pattern,
                info,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if let Some(escape) = escape {
                visit_nested_srfs(
                    escape,
                    info,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
        }
        SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            visit_nested_srfs(
                expr,
                info,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            visit_nested_srfs(
                pattern,
                info,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if let Some(escape) = escape {
                visit_nested_srfs(
                    escape,
                    info,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            if let Some(arg) = arg {
                visit_nested_srfs(arg, info, scope, catalog, outer_scopes, grouped_outer, ctes);
            }
            for arm in args {
                visit_nested_srfs(
                    &arm.expr,
                    info,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                visit_nested_srfs(
                    &arm.result,
                    info,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
            if let Some(defresult) = defresult {
                visit_nested_srfs(
                    defresult,
                    info,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
        }
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::Cast(inner, _)
        | SqlExpr::GeometryUnaryOp { expr: inner, .. }
        | SqlExpr::PrefixOperator { expr: inner, .. }
        | SqlExpr::FieldSelect { expr: inner, .. } => visit_nested_srfs(
            inner,
            info,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        ),
        SqlExpr::Subscript { expr: inner, .. } => visit_nested_srfs(
            inner,
            info,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        ),
        SqlExpr::ArraySubscript { array, subscripts } => {
            visit_nested_srfs(
                array,
                info,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    visit_nested_srfs(
                        lower,
                        info,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                }
                if let Some(upper) = &subscript.upper {
                    visit_nested_srfs(
                        upper,
                        info,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                }
            }
        }
        SqlExpr::GeometryBinaryOp { left, right, .. } => {
            visit_nested_srfs(
                left,
                info,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            visit_nested_srfs(
                right,
                info,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
        }
        SqlExpr::ArrayLiteral(items) | SqlExpr::Row(items) => {
            for item in items {
                visit_nested_srfs(
                    item,
                    info,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
            }
        }
        SqlExpr::InSubquery { expr, .. } => visit_nested_srfs(
            expr,
            info,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        ),
        SqlExpr::QuantifiedSubquery { left, .. } => {
            visit_nested_srfs(
                left,
                info,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
        }
        SqlExpr::QuantifiedArray { left, array, .. } => {
            visit_nested_srfs(
                left,
                info,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            visit_nested_srfs(
                array,
                info,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
        }
        SqlExpr::Xml(xml) => {
            for child in xml.child_exprs() {
                visit_nested_srfs(
                    child,
                    info,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
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
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::CurrentRole
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. } => {}
    }
}

fn func_call_is_set_returning(
    name: &str,
    args: &[SqlFunctionArg],
    func_variadic: bool,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> bool {
    let Ok(lowered_args) = lower_named_table_function_args(name, args) else {
        return false;
    };
    let normalized = name.to_ascii_lowercase();
    if matches!(normalized.as_str(), "generate_series" | "unnest")
        || resolve_json_table_function(&normalized).is_some()
        || resolve_json_record_function(&normalized).is_some_and(|kind| kind.is_set_returning())
        || resolve_regex_table_function(&normalized).is_some()
    {
        return true;
    }
    let actual_types = lowered_args
        .iter()
        .map(|arg| {
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes)
        })
        .collect::<Vec<_>>();
    resolve_function_call(catalog, name, &actual_types, func_variadic)
        .ok()
        .is_some_and(|resolved| resolved.prokind == 'f' && resolved.proretset)
}

fn bind_select_list_srf_call(
    name: &str,
    args: &[SqlFunctionArg],
    func_variadic: bool,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<SetReturningCall, ParseError> {
    let args = lower_named_table_function_args(name, args)?;
    let actual_types = args
        .iter()
        .map(|arg| {
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes)
        })
        .collect::<Vec<_>>();
    let resolved = resolve_function_call(catalog, name, &actual_types, func_variadic).ok();
    let resolved_proc_oid = resolved.as_ref().map(|call| call.proc_oid).unwrap_or(0);
    let resolved_func_variadic = resolved
        .as_ref()
        .map(|call| call.func_variadic)
        .unwrap_or(func_variadic);
    match name.to_ascii_lowercase().as_str() {
        "generate_series" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(ParseError::UnexpectedToken {
                    expected: "generate_series(start, stop[, step])",
                    actual: format!("generate_series with {} arguments", args.len()),
                });
            }
            let start = bind_expr_with_outer_and_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            let stop = bind_expr_with_outer_and_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            let start_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let stop_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let step_type = if args.len() == 3 {
                Some(infer_sql_expr_type_with_ctes(
                    &args[2],
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                ))
            } else {
                None
            };
            let common = resolve_generate_series_common_type(start_type, stop_type, step_type)?;
            let step = if args.len() == 3 {
                let step_expr = bind_expr_with_outer_and_ctes(
                    &args[2],
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let step_type = step_type.expect("generate_series step type");
                coerce_bound_expr(step_expr, step_type, common)
            } else {
                match common.kind {
                    SqlTypeKind::Int8 => Expr::Const(Value::Int64(1)),
                    SqlTypeKind::Numeric => Expr::Const(Value::Numeric(
                        crate::include::nodes::datum::NumericValue::from_i64(1),
                    )),
                    _ => Expr::Const(Value::Int32(1)),
                }
            };
            Ok(SetReturningCall::GenerateSeries {
                func_oid: resolved_proc_oid,
                func_variadic: resolved_func_variadic,
                start: coerce_bound_expr(start, start_type, common),
                stop: coerce_bound_expr(stop, stop_type, common),
                step,
                output_columns: vec![QueryColumn {
                    name: "generate_series".into(),
                    sql_type: common,
                    wire_type_oid: None,
                }],
                with_ordinality: false,
            })
        }
        "unnest" => {
            if args.is_empty() {
                return Err(ParseError::UnexpectedToken {
                    expected: "unnest(array_expr [, array_expr ...])",
                    actual: "unnest()".into(),
                });
            }
            if args.len() > 1 {
                return Err(ParseError::UnexpectedToken {
                    expected: "single-argument unnest(array_expr) in select list",
                    actual: format!("unnest with {} arguments", args.len()),
                });
            }
            let mut bound_args = Vec::with_capacity(args.len());
            let mut output_columns = Vec::with_capacity(args.len());
            for (idx, arg) in args.iter().enumerate() {
                let arg_type = infer_sql_expr_type_with_ctes(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                if !arg_type.is_array && !arg_type.is_multirange() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "array or multirange argument to unnest",
                        actual: format!("{arg:?}"),
                    });
                }
                let element_type = if arg_type.is_multirange() {
                    crate::include::catalog::range_type_ref_for_multirange_sql_type(arg_type)
                        .map(|range_type| range_type.sql_type)
                        .unwrap_or(SqlType::new(SqlTypeKind::Text))
                } else {
                    arg_type.element_type()
                };
                let column_name = if idx == 0 {
                    "unnest".to_string()
                } else {
                    format!("unnest_{}", idx + 1)
                };
                bound_args.push(bind_expr_with_outer_and_ctes(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?);
                output_columns.push(QueryColumn {
                    name: column_name,
                    sql_type: element_type,
                    wire_type_oid: None,
                });
            }
            if output_columns.len() != 1 {
                return Err(ParseError::UnexpectedToken {
                    expected: "single-column set-returning function in select list",
                    actual: name.to_string(),
                });
            }
            Ok(SetReturningCall::Unnest {
                func_oid: resolved_proc_oid,
                func_variadic: resolved_func_variadic,
                args: bound_args,
                output_columns,
                with_ordinality: false,
            })
        }
        other => {
            if let Some(kind) = resolve_json_table_function(other) {
                let bound_args = bind_json_table_srf_args(
                    kind,
                    &args,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let output_columns = match kind {
                    JsonTableFunction::ObjectKeys => vec![QueryColumn::text("json_object_keys")],
                    JsonTableFunction::Each => vec![
                        QueryColumn::text("key"),
                        QueryColumn {
                            name: "value".into(),
                            sql_type: SqlType::new(SqlTypeKind::Json),
                            wire_type_oid: None,
                        },
                    ],
                    JsonTableFunction::EachText => {
                        vec![QueryColumn::text("key"), QueryColumn::text("value")]
                    }
                    JsonTableFunction::ArrayElements => vec![QueryColumn {
                        name: "json_array_elements".into(),
                        sql_type: SqlType::new(SqlTypeKind::Json),
                        wire_type_oid: None,
                    }],
                    JsonTableFunction::ArrayElementsText => {
                        vec![QueryColumn::text("json_array_elements_text")]
                    }
                    JsonTableFunction::JsonbPathQuery => vec![QueryColumn {
                        name: "jsonb_path_query".into(),
                        sql_type: SqlType::new(SqlTypeKind::Jsonb),
                        wire_type_oid: None,
                    }],
                    JsonTableFunction::JsonbObjectKeys => {
                        vec![QueryColumn::text("jsonb_object_keys")]
                    }
                    JsonTableFunction::JsonbEach => vec![
                        QueryColumn::text("key"),
                        QueryColumn {
                            name: "value".into(),
                            sql_type: SqlType::new(SqlTypeKind::Jsonb),
                            wire_type_oid: None,
                        },
                    ],
                    JsonTableFunction::JsonbEachText => {
                        vec![QueryColumn::text("key"), QueryColumn::text("value")]
                    }
                    JsonTableFunction::JsonbArrayElements => vec![QueryColumn {
                        name: "jsonb_array_elements".into(),
                        sql_type: SqlType::new(SqlTypeKind::Jsonb),
                        wire_type_oid: None,
                    }],
                    JsonTableFunction::JsonbArrayElementsText => {
                        vec![QueryColumn::text("jsonb_array_elements_text")]
                    }
                };
                Ok(SetReturningCall::JsonTableFunction {
                    func_oid: resolved_proc_oid,
                    func_variadic: resolved_func_variadic,
                    kind,
                    args: bound_args,
                    output_columns,
                    with_ordinality: false,
                })
            } else if let Some(kind) = resolve_json_record_function(other) {
                if !kind.is_set_returning() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "supported set-returning function",
                        actual: other.to_string(),
                    });
                }
                let resolved = resolved
                    .as_ref()
                    .ok_or_else(|| ParseError::UnexpectedToken {
                        expected: "supported set-returning function",
                        actual: other.to_string(),
                    })?;
                let bound_args = bind_user_defined_srf_args(
                    &args,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                    &resolved.declared_arg_types,
                )?;
                let output_columns = vec![QueryColumn {
                    name: other.to_string(),
                    sql_type: resolved.result_type,
                    wire_type_oid: None,
                }];
                if matches!(resolved.result_type.kind, SqlTypeKind::Record)
                    && resolved.result_type.typmod > 0
                    && lookup_anonymous_record_descriptor(resolved.result_type.typmod).is_none()
                {
                    return Err(ParseError::UnexpectedToken {
                        expected: "registered anonymous record descriptor",
                        actual: other.to_string(),
                    });
                }
                Ok(SetReturningCall::JsonRecordFunction {
                    func_oid: resolved.proc_oid,
                    func_variadic: resolved.func_variadic,
                    kind,
                    args: bound_args,
                    output_columns,
                    record_type: Some(resolved.result_type),
                    with_ordinality: false,
                })
            } else {
                if let Some(kind) = resolve_regex_table_function(other) {
                    let bound_args = args
                        .iter()
                        .map(|arg| {
                            bind_expr_with_outer_and_ctes(
                                arg,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                ctes,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let output_columns = match kind {
                        crate::include::nodes::primnodes::RegexTableFunction::Matches => {
                            vec![QueryColumn {
                                name: "regexp_matches".into(),
                                sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                                wire_type_oid: None,
                            }]
                        }
                        crate::include::nodes::primnodes::RegexTableFunction::SplitToTable => {
                            vec![QueryColumn::text("regexp_split_to_table")]
                        }
                    };
                    Ok(SetReturningCall::RegexTableFunction {
                        func_oid: resolved_proc_oid,
                        func_variadic: resolved_func_variadic,
                        kind,
                        args: bound_args,
                        output_columns,
                        with_ordinality: false,
                    })
                } else if let Some(resolved) = resolved.as_ref() {
                    if resolved.prokind != 'f' || !resolved.proretset {
                        return Err(ParseError::UnexpectedToken {
                            expected: "supported set-returning function",
                            actual: other.to_string(),
                        });
                    }
                    if !matches!(resolved.row_shape, ResolvedFunctionRowShape::None) {
                        return Err(ParseError::UnexpectedToken {
                            expected: "scalar-output set-returning function in select list",
                            actual: other.to_string(),
                        });
                    }
                    let bound_args = bind_user_defined_srf_args(
                        &args,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                        &resolved.declared_arg_types,
                    )?;
                    let output_columns = vec![QueryColumn {
                        name: other.to_string(),
                        sql_type: resolved.result_type,
                        wire_type_oid: None,
                    }];
                    Ok(SetReturningCall::UserDefined {
                        proc_oid: resolved.proc_oid,
                        func_variadic: resolved.func_variadic,
                        args: bound_args,
                        output_columns,
                        with_ordinality: false,
                    })
                } else {
                    Err(ParseError::UnexpectedToken {
                        expected: "supported set-returning function",
                        actual: other.to_string(),
                    })
                }
            }
        }
    }
}

fn bind_json_table_srf_args(
    kind: JsonTableFunction,
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Vec<Expr>, ParseError> {
    let target_type = match kind {
        JsonTableFunction::JsonbEach | JsonTableFunction::JsonbEachText => {
            Some(SqlType::new(SqlTypeKind::Jsonb))
        }
        _ => None,
    };
    args.iter()
        .map(|arg| {
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                arg,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let resolved_arg_type = target_type
                .map(|target| coerce_unknown_string_literal_type(arg, raw_arg_type, target))
                .unwrap_or(raw_arg_type);
            let bound = bind_expr_with_outer_and_ctes(
                arg,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            Ok(match target_type {
                Some(target) if resolved_arg_type == target && raw_arg_type != target => {
                    coerce_bound_expr(bound, raw_arg_type, target)
                }
                None => bound,
                Some(_) => bound,
            })
        })
        .collect()
}

fn bind_user_defined_srf_args(
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
    declared_arg_types: &[SqlType],
) -> Result<Vec<Expr>, ParseError> {
    let arg_types = args
        .iter()
        .map(|arg| {
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes)
        })
        .collect::<Vec<_>>();
    let bound_args = args
        .iter()
        .map(|arg| {
            bind_expr_with_outer_and_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(bound_args
        .into_iter()
        .zip(arg_types)
        .zip(declared_arg_types.iter().copied())
        .map(|((arg, actual_type), declared_type)| {
            coerce_bound_expr(arg, actual_type, declared_type)
        })
        .collect())
}

fn expand_star_targets(
    scope: &BoundScope,
    relation: Option<&str>,
) -> Result<Vec<TargetEntry>, ParseError> {
    let entries = scope
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| {
            !column.hidden
                && relation.is_none_or(|relation_name| {
                    column
                        .relation_names
                        .iter()
                        .any(|visible| visible.eq_ignore_ascii_case(relation_name))
                })
        })
        .map(|(index, column)| {
            TargetEntry::new(
                column.output_name.clone(),
                scope.output_exprs.get(index).cloned().unwrap_or_else(|| {
                    panic!("bound scope output_exprs missing star expansion column {index}")
                }),
                scope.desc.columns[index].sql_type,
                index + 1,
            )
            .with_input_resno(index + 1)
        })
        .collect::<Vec<_>>();

    let relation_exists = relation.is_some_and(|relation_name| {
        scope.columns.iter().any(|column| {
            column
                .relation_names
                .iter()
                .any(|visible| visible.eq_ignore_ascii_case(relation_name))
        })
    });

    if entries.is_empty() && relation.is_some() && !relation_exists {
        return Err(ParseError::UnknownColumn(
            relation
                .map(|name| format!("{name}.*"))
                .unwrap_or_else(|| "*".to_string()),
        ));
    }
    Ok(entries)
}
