use super::*;

pub(crate) enum BoundSelectTargets {
    Plain(Vec<TargetEntry>),
    WithProjectSet {
        project_targets: Vec<ProjectSetTarget>,
        final_targets: Vec<TargetEntry>,
    },
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
        let info = classify_select_target_srf(&item.expr);
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
            ProjectSetTarget::Scalar(TargetEntry {
                name: column.output_name.clone(),
                expr: Expr::Column(index),
                sql_type: scope.desc.columns[index].sql_type,
            })
        })
        .collect::<Vec<_>>();

    let mut final_targets = Vec::new();
    let mut srf_index = 0usize;
    let base_width = scope.columns.len();

    for item in targets {
        if let Some((name, args, func_variadic)) = top_level_set_returning_call(&item.expr) {
            let (call, sql_type) = bind_select_list_srf_call(
                &name,
                &args,
                func_variadic,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            let output_name = item.output_name.clone();
            project_targets.push(ProjectSetTarget::Set {
                name: output_name.clone(),
                call,
                sql_type,
                column_index: 0,
            });
            final_targets.push(TargetEntry {
                name: output_name,
                expr: Expr::Column(base_width + srf_index),
                sql_type,
            });
            srf_index += 1;
            continue;
        }

        final_targets.push(TargetEntry {
            name: item.output_name.clone(),
            expr: bind_expr_with_outer_and_ctes(
                &item.expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            sql_type: infer_sql_expr_type_with_ctes(
                &item.expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        });
    }

    Ok(BoundSelectTargets::WithProjectSet {
        project_targets,
        final_targets,
    })
}

pub(crate) fn select_targets_contain_set_returning_call(targets: &[SelectItem]) -> bool {
    targets
        .iter()
        .any(|item| classify_select_target_srf(&item.expr).top_level.is_some())
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

        entries.push(TargetEntry {
            name: item.output_name.clone(),
            expr: bind_expr_with_outer_and_ctes(
                &item.expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            sql_type: infer_sql_expr_type_with_ctes(
                &item.expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        });
    }
    Ok(entries)
}

#[derive(Default)]
struct TargetSrfInfo {
    top_level: Option<(String, Vec<SqlFunctionArg>, bool)>,
    has_nested: bool,
}

fn classify_select_target_srf(expr: &SqlExpr) -> TargetSrfInfo {
    match expr {
        SqlExpr::FuncCall {
            name,
            args,
            func_variadic,
        } if set_returning_function_name(name).is_some() => TargetSrfInfo {
            top_level: Some((name.clone(), args.clone(), *func_variadic)),
            has_nested: false,
        },
        _ => {
            let mut info = TargetSrfInfo::default();
            visit_nested_srfs(expr, &mut info);
            info
        }
    }
}

fn top_level_set_returning_call(expr: &SqlExpr) -> Option<(String, Vec<SqlFunctionArg>, bool)> {
    match expr {
        SqlExpr::FuncCall {
            name,
            args,
            func_variadic,
        } if set_returning_function_name(name).is_some() => {
            Some((name.clone(), args.clone(), *func_variadic))
        }
        _ => None,
    }
}

fn visit_nested_srfs(expr: &SqlExpr, info: &mut TargetSrfInfo) {
    match expr {
        SqlExpr::FuncCall { name, args, .. } => {
            if set_returning_function_name(name).is_some() {
                info.has_nested = true;
            }
            for arg in args {
                visit_nested_srfs(&arg.value, info);
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
            visit_nested_srfs(left, info);
            visit_nested_srfs(right, info);
        }
        SqlExpr::BinaryOperator { left, right, .. } => {
            visit_nested_srfs(left, info);
            visit_nested_srfs(right, info);
        }
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            visit_nested_srfs(expr, info);
            visit_nested_srfs(pattern, info);
            if let Some(escape) = escape {
                visit_nested_srfs(escape, info);
            }
        }
        SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            visit_nested_srfs(expr, info);
            visit_nested_srfs(pattern, info);
            if let Some(escape) = escape {
                visit_nested_srfs(escape, info);
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
        | SqlExpr::FieldSelect { expr: inner, .. } => visit_nested_srfs(inner, info),
        | SqlExpr::Subscript { expr: inner, .. } => visit_nested_srfs(inner, info),
        SqlExpr::ArraySubscript { array, subscripts } => {
            visit_nested_srfs(array, info);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    visit_nested_srfs(lower, info);
                }
                if let Some(upper) = &subscript.upper {
                    visit_nested_srfs(upper, info);
                }
            }
        }
        SqlExpr::GeometryBinaryOp { left, right, .. } => {
            visit_nested_srfs(left, info);
            visit_nested_srfs(right, info);
        }
        SqlExpr::ArrayLiteral(items) => {
            for item in items {
                visit_nested_srfs(item, info);
            }
        }
        SqlExpr::AggCall { args, .. } => {
            for arg in args {
                visit_nested_srfs(&arg.value, info);
            }
        }
        SqlExpr::InSubquery { expr, .. } => visit_nested_srfs(expr, info),
        SqlExpr::QuantifiedSubquery { left, .. } => {
            visit_nested_srfs(left, info);
        }
        SqlExpr::QuantifiedArray { left, array, .. } => {
            visit_nested_srfs(left, info);
            visit_nested_srfs(array, info);
        }
        SqlExpr::Column(_)
        | SqlExpr::Default
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::ScalarSubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. } => {}
    }
}

fn set_returning_function_name(name: &str) -> Option<&str> {
    match name.to_ascii_lowercase().as_str() {
        "generate_series"
        | "unnest"
        | "json_object_keys"
        | "json_each"
        | "json_each_text"
        | "json_array_elements"
        | "json_array_elements_text"
        | "jsonb_object_keys"
        | "jsonb_each"
        | "jsonb_each_text"
        | "jsonb_path_query"
        | "jsonb_array_elements"
        | "jsonb_array_elements_text"
        | "regexp_matches"
        | "regexp_split_to_table" => Some(name),
        _ => None,
    }
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
) -> Result<(SetReturningCall, SqlType), ParseError> {
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
            let common = resolve_numeric_binary_type("+", start_type, stop_type)?;
            if !matches!(
                common.kind,
                SqlTypeKind::Int4 | SqlTypeKind::Int8 | SqlTypeKind::Numeric
            ) {
                return Err(ParseError::UnexpectedToken {
                    expected: "generate_series integer or numeric arguments",
                    actual: sql_type_name(common),
                });
            }
            let step = if args.len() == 3 {
                let step_expr = bind_expr_with_outer_and_ctes(
                    &args[2],
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let step_type = infer_sql_expr_type_with_ctes(
                    &args[2],
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
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
            Ok((
                SetReturningCall::GenerateSeries {
                    func_oid: resolved_proc_oid,
                    func_variadic: resolved_func_variadic,
                    start: coerce_bound_expr(start, start_type, common),
                    stop: coerce_bound_expr(stop, stop_type, common),
                    step,
                    output: QueryColumn {
                        name: "generate_series".into(),
                        sql_type: common,
                    },
                },
                common,
            ))
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
                if !arg_type.is_array {
                    return Err(ParseError::UnexpectedToken {
                        expected: "array argument to unnest",
                        actual: format!("{arg:?}"),
                    });
                }
                let element_type = arg_type.element_type();
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
                });
            }
            if output_columns.len() != 1 {
                return Err(ParseError::UnexpectedToken {
                    expected: "single-column set-returning function in select list",
                    actual: name.to_string(),
                });
            }
            Ok((
                SetReturningCall::Unnest {
                    func_oid: resolved_proc_oid,
                    func_variadic: resolved_func_variadic,
                    args: bound_args,
                    output_columns: output_columns.clone(),
                },
                output_columns[0].sql_type,
            ))
        }
        other => {
            if let Some(kind) = resolve_json_table_function(other) {
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
                    JsonTableFunction::ObjectKeys => vec![QueryColumn::text("json_object_keys")],
                    JsonTableFunction::ArrayElements => vec![QueryColumn {
                        name: "json_array_elements".into(),
                        sql_type: SqlType::new(SqlTypeKind::Json),
                    }],
                    JsonTableFunction::ArrayElementsText => {
                        vec![QueryColumn::text("json_array_elements_text")]
                    }
                    JsonTableFunction::JsonbPathQuery => vec![QueryColumn {
                        name: "jsonb_path_query".into(),
                        sql_type: SqlType::new(SqlTypeKind::Jsonb),
                    }],
                    JsonTableFunction::JsonbObjectKeys => {
                        vec![QueryColumn::text("jsonb_object_keys")]
                    }
                    JsonTableFunction::JsonbArrayElements => vec![QueryColumn {
                        name: "jsonb_array_elements".into(),
                        sql_type: SqlType::new(SqlTypeKind::Jsonb),
                    }],
                    JsonTableFunction::JsonbArrayElementsText => {
                        vec![QueryColumn::text("jsonb_array_elements_text")]
                    }
                    JsonTableFunction::Each
                    | JsonTableFunction::EachText
                    | JsonTableFunction::JsonbEach
                    | JsonTableFunction::JsonbEachText => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "scalar-output set-returning function in select list",
                            actual: other.to_string(),
                        });
                    }
                };
                Ok((
                    SetReturningCall::JsonTableFunction {
                        func_oid: resolved_proc_oid,
                        func_variadic: resolved_func_variadic,
                        kind,
                        args: bound_args,
                        output_columns: output_columns.clone(),
                    },
                    output_columns[0].sql_type,
                ))
            } else {
                let kind = resolve_regex_table_function(other).ok_or_else(|| {
                    ParseError::UnexpectedToken {
                        expected: "supported set-returning function",
                        actual: other.to_string(),
                    }
                })?;
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
                    crate::include::nodes::plannodes::RegexTableFunction::Matches => {
                        vec![QueryColumn {
                            name: "regexp_matches".into(),
                            sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                        }]
                    }
                    crate::include::nodes::plannodes::RegexTableFunction::SplitToTable => {
                        vec![QueryColumn::text("regexp_split_to_table")]
                    }
                };
                Ok((
                    SetReturningCall::RegexTableFunction {
                        func_oid: resolved_proc_oid,
                        func_variadic: resolved_func_variadic,
                        kind,
                        args: bound_args,
                        output_columns: output_columns.clone(),
                    },
                    output_columns[0].sql_type,
                ))
            }
        }
    }
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
            relation.is_none_or(|relation_name| {
                column
                    .relation_names
                    .iter()
                    .any(|visible| visible.eq_ignore_ascii_case(relation_name))
            })
        })
        .map(|(index, column)| TargetEntry {
            name: column.output_name.clone(),
            expr: Expr::Column(index),
            sql_type: scope.desc.columns[index].sql_type,
        })
        .collect::<Vec<_>>();

    if entries.is_empty() {
        return Err(ParseError::UnknownColumn(
            relation
                .map(|name| format!("{name}.*"))
                .unwrap_or_else(|| "*".to_string()),
        ));
    }
    Ok(entries)
}
