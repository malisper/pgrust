use super::*;
use crate::backend::utils::record::{
    assign_anonymous_record_descriptor, lookup_anonymous_record_descriptor,
};
pub(crate) enum BoundSelectTargets {
    Plain(Vec<TargetEntry>),
}

struct BoundSelectListSrfTarget {
    output_name: String,
    call: SetReturningCall,
    sql_type: SqlType,
    column_index: usize,
}

struct BoundScalarSelectTarget {
    output_name: String,
    expr: Expr,
    sql_type: SqlType,
    input_resno: Option<usize>,
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
    let mut entries = Vec::with_capacity(targets.len());
    for item in targets {
        let item_targets =
            bind_select_item_once(item, scope, catalog, outer_scopes, grouped_outer, ctes)?;
        for bound in item_targets {
            entries.push(
                TargetEntry::new(
                    bound.output_name,
                    bound.expr,
                    bound.sql_type,
                    entries.len() + 1,
                )
                .with_input_resno_opt(bound.input_resno),
            );
        }
    }
    Ok(BoundSelectTargets::Plain(entries))
}

fn bind_select_item_once(
    item: &SelectItem,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Vec<BoundScalarSelectTarget>, ParseError> {
    if let SqlExpr::Column(name) = &item.expr {
        if name == "*" {
            return Ok(bound_scalar_items_from_target_entries(expand_star_targets(
                scope, None,
            )?));
        }
        if let Some(relation) = name.strip_suffix(".*") {
            return Ok(bound_scalar_items_from_target_entries(
                expand_named_star_targets(scope, outer_scopes, relation)?,
            ));
        }
    }
    if let SqlExpr::FieldSelect { expr, field } = &item.expr
        && field == "*"
    {
        return Ok(bound_scalar_items_from_target_entries(
            expand_record_expr_targets(expr, scope, catalog, outer_scopes, grouped_outer, ctes, 1)?,
        ));
    }

    let typed = bind_typed_expr_with_outer_and_ctes(
        &item.expr,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    let input_resno = input_resno_for_scope_expr(scope, &typed.expr);
    Ok(vec![BoundScalarSelectTarget {
        output_name: select_item_output_name(item),
        expr: typed.expr,
        sql_type: typed.sql_type,
        input_resno,
    }])
}

fn select_item_output_name(item: &SelectItem) -> String {
    if item.output_name == "?column?"
        && let SqlExpr::ScalarSubquery(select) = &item.expr
        && let Some(target) = select.targets.first()
    {
        return target.output_name.clone();
    }
    item.output_name.clone()
}

fn bound_scalar_items_from_target_entries(
    entries: Vec<TargetEntry>,
) -> Vec<BoundScalarSelectTarget> {
    entries
        .into_iter()
        .map(|entry| BoundScalarSelectTarget {
            output_name: entry.name,
            expr: entry.expr,
            sql_type: entry.sql_type,
            input_resno: entry.input_resno,
        })
        .collect()
}

fn expand_named_star_targets(
    scope: &BoundScope,
    outer_scopes: &[BoundScope],
    relation: &str,
) -> Result<Vec<TargetEntry>, ParseError> {
    match expand_star_targets(scope, Some(relation)) {
        Ok(entries) => Ok(entries),
        Err(ParseError::UnknownColumn(_)) => outer_scopes
            .iter()
            .enumerate()
            .find_map(|(depth, outer_scope)| {
                match expand_star_targets(outer_scope, Some(relation)) {
                    Ok(entries) => Some(Ok(entries
                        .into_iter()
                        .map(|entry| TargetEntry {
                            expr: raise_expr_varlevels(entry.expr, depth + 1),
                            input_resno: None,
                            ..entry
                        })
                        .collect())),
                    Err(ParseError::UnknownColumn(_)) => None,
                    Err(err) => Some(Err(err)),
                }
            })
            .unwrap_or_else(|| Err(ParseError::UnknownColumn(format!("{relation}.*")))),
        Err(err) => Err(err),
    }
}

fn expand_record_expr_targets(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
    resno_start: usize,
) -> Result<Vec<TargetEntry>, ParseError> {
    let bound_expr =
        bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, grouped_outer, ctes)?;
    let fields = record_expr_fields(&bound_expr, catalog)?;
    Ok(fields
        .into_iter()
        .enumerate()
        .map(|(index, (name, sql_type))| {
            TargetEntry::new(
                name.clone(),
                Expr::FieldSelect {
                    expr: Box::new(bound_expr.clone()),
                    field: name,
                    field_type: sql_type,
                },
                sql_type,
                resno_start + index,
            )
        })
        .collect())
}

fn record_expr_fields(
    expr: &Expr,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<(String, SqlType)>, ParseError> {
    if let Expr::Row { descriptor, .. } = expr {
        return Ok(descriptor
            .fields
            .iter()
            .map(|field| (field.name.clone(), field.sql_type))
            .collect());
    }

    let Some(sql_type) = expr_sql_type_hint(expr) else {
        return Err(ParseError::UnexpectedToken {
            expected: "record expression",
            actual: "field expansion .*".into(),
        });
    };

    if matches!(sql_type.kind, SqlTypeKind::Composite) && sql_type.typrelid != 0 {
        let relation = catalog
            .lookup_relation_by_oid(sql_type.typrelid)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "named composite type",
                actual: format!("type relation {} not found", sql_type.typrelid),
            })?;
        return Ok(relation
            .desc
            .columns
            .into_iter()
            .filter(|column| !column.dropped)
            .map(|column| (column.name, column.sql_type))
            .collect());
    }

    if matches!(sql_type.kind, SqlTypeKind::Record)
        && sql_type.typmod > 0
        && let Some(descriptor) = lookup_anonymous_record_descriptor(sql_type.typmod)
    {
        return Ok(descriptor
            .fields
            .into_iter()
            .map(|field| (field.name, field.sql_type))
            .collect());
    }

    Err(ParseError::UnexpectedToken {
        expected: "record expression",
        actual: "field expansion .*".into(),
    })
}

fn bind_select_list_srf_target_from_parts(
    output_name: String,
    name: &str,
    args: &[SqlFunctionArg],
    func_variadic: bool,
    projected_field: Option<&str>,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<BoundSelectListSrfTarget, ParseError> {
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
        output_name,
        call,
        sql_type,
        column_index,
    })
}

pub(super) fn bind_set_returning_expr_from_parts(
    name: &str,
    args: &[SqlFunctionArg],
    func_variadic: bool,
    projected_field: Option<&str>,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let output_name = projected_field
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| name.to_ascii_lowercase());
    let bound = bind_select_list_srf_target_from_parts(
        output_name.clone(),
        name,
        args,
        func_variadic,
        projected_field,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    Ok(Expr::set_returning(
        output_name,
        bound.call,
        bound.sql_type,
        bound.column_index,
    ))
}

pub(super) fn root_call_returns_set(
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
    if matches!(
        normalized.as_str(),
        "generate_series" | "generate_subscripts" | "unnest"
    ) || resolve_json_table_function(&normalized).is_some()
        || resolve_json_record_function(&normalized).is_some_and(|kind| kind.is_set_returning())
        || resolve_regex_table_function(&normalized).is_some()
        || resolve_string_table_function(&normalized).is_some()
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
            if args.len() < 2 || args.len() > 4 {
                return Err(ParseError::UnexpectedToken {
                    expected: "generate_series(start, stop, step[, timezone])",
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
            let step_type = if args.len() >= 3 {
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
            let timezone_type = if args.len() == 4 {
                Some(infer_sql_expr_type_with_ctes(
                    &args[3],
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
            if timezone_type.is_some() && !matches!(common.kind, SqlTypeKind::TimestampTz) {
                return Err(ParseError::UnexpectedToken {
                    expected: "generate_series timestamptz arguments with timezone",
                    actual: sql_type_name(common),
                });
            }
            let step = if args.len() >= 3 {
                let step_expr = bind_expr_with_outer_and_ctes(
                    &args[2],
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let step_type = step_type.expect("generate_series step type");
                let step_target = if matches!(
                    common.kind,
                    SqlTypeKind::Timestamp | SqlTypeKind::TimestampTz
                ) {
                    SqlType::new(SqlTypeKind::Interval)
                } else {
                    common
                };
                coerce_bound_expr(step_expr, step_type, step_target)
            } else {
                match common.kind {
                    SqlTypeKind::Int8 => Expr::Const(Value::Int64(1)),
                    SqlTypeKind::Numeric => Expr::Const(Value::Numeric(
                        crate::include::nodes::datum::NumericValue::from_i64(1),
                    )),
                    _ => Expr::Const(Value::Int32(1)),
                }
            };
            let timezone = if args.len() == 4 {
                let timezone_expr = bind_expr_with_outer_and_ctes(
                    &args[3],
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                Some(coerce_bound_expr(
                    timezone_expr,
                    timezone_type.expect("generate_series timezone type"),
                    SqlType::new(SqlTypeKind::Text),
                ))
            } else {
                None
            };
            Ok(SetReturningCall::GenerateSeries {
                func_oid: resolved_proc_oid,
                func_variadic: resolved_func_variadic,
                start: coerce_bound_expr(start, start_type, common),
                stop: coerce_bound_expr(stop, stop_type, common),
                step,
                timezone,
                output_columns: vec![QueryColumn {
                    name: "generate_series".into(),
                    sql_type: common,
                    wire_type_oid: None,
                }],
                with_ordinality: false,
            })
        }
        "generate_subscripts" => {
            if !(2..=3).contains(&args.len()) {
                return Err(ParseError::UnexpectedToken {
                    expected: "generate_subscripts(array, dimension [, reverse])",
                    actual: format!("generate_subscripts with {} arguments", args.len()),
                });
            }
            let array_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !array_type.is_array
                && !matches!(
                    array_type.kind,
                    SqlTypeKind::Int2Vector | SqlTypeKind::OidVector
                )
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "array argument to generate_subscripts",
                    actual: sql_type_name(array_type),
                });
            }
            let dimension_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let array = bind_expr_with_outer_and_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            let dimension = bind_expr_with_outer_and_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            let reverse = if args.len() == 3 {
                let reverse_type = infer_sql_expr_type_with_ctes(
                    &args[2],
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                Some(coerce_bound_expr(
                    bind_expr_with_outer_and_ctes(
                        &args[2],
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?,
                    reverse_type,
                    SqlType::new(SqlTypeKind::Bool),
                ))
            } else {
                None
            };
            Ok(SetReturningCall::GenerateSubscripts {
                func_oid: resolved_proc_oid,
                func_variadic: resolved_func_variadic,
                array,
                dimension: coerce_bound_expr(
                    dimension,
                    dimension_type,
                    SqlType::new(SqlTypeKind::Int4),
                ),
                reverse,
                output_columns: vec![QueryColumn {
                    name: "generate_subscripts".into(),
                    sql_type: SqlType::new(SqlTypeKind::Int4),
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
                if !arg_type.is_array && matches!(arg_type.kind, SqlTypeKind::TsVector) {
                    bound_args.push(bind_expr_with_outer_and_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?);
                    output_columns.extend([
                        QueryColumn {
                            name: "lexeme".into(),
                            sql_type: SqlType::new(SqlTypeKind::Text),
                            wire_type_oid: None,
                        },
                        QueryColumn {
                            name: "positions".into(),
                            sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Int2)),
                            wire_type_oid: None,
                        },
                        QueryColumn {
                            name: "weights".into(),
                            sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                            wire_type_oid: None,
                        },
                    ]);
                    continue;
                }
                let Some(element_type) = unnest_element_type(arg_type) else {
                    return Err(ParseError::UnexpectedToken {
                        expected: "array or multirange argument to unnest",
                        actual: format!("{arg:?}"),
                    });
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
                        name: "value".into(),
                        sql_type: SqlType::new(SqlTypeKind::Json),
                        wire_type_oid: None,
                    }],
                    JsonTableFunction::ArrayElementsText => {
                        vec![QueryColumn::text("value")]
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
                        name: "value".into(),
                        sql_type: SqlType::new(SqlTypeKind::Jsonb),
                        wire_type_oid: None,
                    }],
                    JsonTableFunction::JsonbArrayElementsText => {
                        vec![QueryColumn::text("value")]
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
                } else if let Some(kind) = resolve_string_table_function(other) {
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
                    let output_columns = vec![QueryColumn::text("string_to_table")];
                    Ok(SetReturningCall::StringTableFunction {
                        func_oid: resolved_proc_oid,
                        func_variadic: resolved_func_variadic,
                        kind,
                        args: bound_args,
                        output_columns,
                        with_ordinality: false,
                    })
                } else if let Some(resolved) = resolved.as_ref() {
                    if matches!(resolved.srf_impl, Some(ResolvedSrfImpl::PgLockStatus)) {
                        if !args.is_empty() {
                            return Err(ParseError::UnexpectedToken {
                                expected: "pg_lock_status()",
                                actual: format!("pg_lock_status with {} arguments", args.len()),
                            });
                        }
                        let output_columns = match &resolved.row_shape {
                            ResolvedFunctionRowShape::OutParameters(columns)
                            | ResolvedFunctionRowShape::NamedComposite { columns, .. } => {
                                columns.clone()
                            }
                            ResolvedFunctionRowShape::AnonymousRecord
                            | ResolvedFunctionRowShape::None => {
                                return Err(ParseError::UnexpectedToken {
                                    expected: "pg_lock_status OUT parameter metadata",
                                    actual: other.to_string(),
                                });
                            }
                        };
                        return Ok(SetReturningCall::PgLockStatus {
                            func_oid: resolved.proc_oid,
                            func_variadic: resolved.func_variadic,
                            output_columns,
                            with_ordinality: false,
                        });
                    }
                    if let Some(
                        srf_impl @ (ResolvedSrfImpl::PartitionTree
                        | ResolvedSrfImpl::PartitionAncestors),
                    ) = resolved.srf_impl
                    {
                        let bound_args = bind_user_defined_srf_args(
                            &args,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                            &resolved.declared_arg_types,
                        )?;
                        let output_columns = match &resolved.row_shape {
                            ResolvedFunctionRowShape::OutParameters(columns)
                            | ResolvedFunctionRowShape::NamedComposite { columns, .. } => {
                                columns.clone()
                            }
                            ResolvedFunctionRowShape::AnonymousRecord
                            | ResolvedFunctionRowShape::None => match srf_impl {
                                ResolvedSrfImpl::PartitionTree => vec![
                                    QueryColumn {
                                        name: "relid".into(),
                                        sql_type: SqlType::new(SqlTypeKind::RegClass),
                                        wire_type_oid: None,
                                    },
                                    QueryColumn {
                                        name: "parentrelid".into(),
                                        sql_type: SqlType::new(SqlTypeKind::RegClass),
                                        wire_type_oid: None,
                                    },
                                    QueryColumn {
                                        name: "isleaf".into(),
                                        sql_type: SqlType::new(SqlTypeKind::Bool),
                                        wire_type_oid: None,
                                    },
                                    QueryColumn {
                                        name: "level".into(),
                                        sql_type: SqlType::new(SqlTypeKind::Int4),
                                        wire_type_oid: None,
                                    },
                                ],
                                ResolvedSrfImpl::PartitionAncestors => vec![QueryColumn {
                                    name: "relid".into(),
                                    sql_type: SqlType::new(SqlTypeKind::RegClass),
                                    wire_type_oid: None,
                                }],
                                _ => unreachable!(
                                    "partition SRF branch only handles partition builtins"
                                ),
                            },
                        };
                        let relid = bound_args.into_iter().next().ok_or_else(|| {
                            ParseError::UnexpectedToken {
                                expected: "single regclass argument",
                                actual: other.to_string(),
                            }
                        })?;
                        return Ok(match srf_impl {
                            ResolvedSrfImpl::PartitionTree => SetReturningCall::PartitionTree {
                                func_oid: resolved.proc_oid,
                                func_variadic: resolved.func_variadic,
                                relid,
                                output_columns,
                            },
                            ResolvedSrfImpl::PartitionAncestors => {
                                SetReturningCall::PartitionAncestors {
                                    func_oid: resolved.proc_oid,
                                    func_variadic: resolved.func_variadic,
                                    relid,
                                    output_columns,
                                }
                            }
                            _ => {
                                unreachable!("partition SRF branch only handles partition builtins")
                            }
                        });
                    }
                    if matches!(resolved.srf_impl, Some(ResolvedSrfImpl::TxidSnapshotXip)) {
                        let bound_args = bind_user_defined_srf_args(
                            &args,
                            scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                            &resolved.declared_arg_types,
                        )?;
                        let arg = bound_args.into_iter().next().ok_or_else(|| {
                            ParseError::UnexpectedToken {
                                expected: "single txid_snapshot argument",
                                actual: other.to_string(),
                            }
                        })?;
                        return Ok(SetReturningCall::TxidSnapshotXip {
                            func_oid: resolved.proc_oid,
                            func_variadic: resolved.func_variadic,
                            arg,
                            output_columns: vec![QueryColumn {
                                name: other.to_string(),
                                sql_type: resolved.result_type,
                                wire_type_oid: None,
                            }],
                            with_ordinality: false,
                        });
                    }
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

fn unnest_element_type(arg_type: SqlType) -> Option<SqlType> {
    if arg_type.is_array {
        return Some(arg_type.element_type());
    }
    if arg_type.is_multirange() {
        return Some(
            crate::include::catalog::range_type_ref_for_multirange_sql_type(arg_type)
                .map(|range_type| range_type.sql_type)
                .unwrap_or(SqlType::new(SqlTypeKind::Text)),
        );
    }
    match arg_type.kind {
        SqlTypeKind::Int2Vector => Some(SqlType::new(SqlTypeKind::Int2)),
        SqlTypeKind::OidVector => Some(SqlType::new(SqlTypeKind::Oid)),
        _ => None,
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
    args.iter()
        .enumerate()
        .map(|(index, arg)| {
            let target_type = match (kind, index) {
                (JsonTableFunction::JsonbPathQuery, 0 | 2)
                | (JsonTableFunction::JsonbObjectKeys, 0)
                | (JsonTableFunction::JsonbEach, 0)
                | (JsonTableFunction::JsonbEachText, 0)
                | (JsonTableFunction::JsonbArrayElements, 0)
                | (JsonTableFunction::JsonbArrayElementsText, 0) => {
                    Some(SqlType::new(SqlTypeKind::Jsonb))
                }
                (JsonTableFunction::JsonbPathQuery, 1) => Some(SqlType::new(SqlTypeKind::JsonPath)),
                (JsonTableFunction::JsonbPathQuery, 3) => Some(SqlType::new(SqlTypeKind::Bool)),
                _ => None,
            };
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
