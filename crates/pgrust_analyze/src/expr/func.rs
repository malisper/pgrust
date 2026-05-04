use super::*;
use pgrust_catalog_data::{
    ANYARRAYOID, ANYCOMPATIBLEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID, ANYNONARRAYOID,
    ANYOID, ANYRANGEOID, PG_LANGUAGE_SQL_OID, PgProcRow, UNKNOWN_TYPE_OID,
    range_type_ref_for_sql_type,
};
use pgrust_nodes::datum::RecordDescriptor;
use pgrust_nodes::primnodes::expr_sql_type_hint;
use pgrust_nodes::record::assign_anonymous_record_descriptor;
use pgrust_parser::parse_statement;

fn signed_integer_literal_type(expr: &SqlExpr) -> Option<SqlTypeKind> {
    let (negative, value) = match expr {
        SqlExpr::IntegerLiteral(value) => (false, value.as_str()),
        SqlExpr::Negate(inner) => match inner.as_ref() {
            SqlExpr::IntegerLiteral(value) => (true, value.as_str()),
            _ => return None,
        },
        SqlExpr::UnaryPlus(inner) => match inner.as_ref() {
            SqlExpr::IntegerLiteral(value) => (false, value.as_str()),
            _ => return None,
        },
        _ => return None,
    };
    if !value.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let signed = if negative {
        format!("-{value}")
    } else {
        value.to_string()
    };
    if signed.parse::<i32>().is_ok() {
        Some(SqlTypeKind::Int4)
    } else if signed.parse::<i64>().is_ok() {
        Some(SqlTypeKind::Int8)
    } else {
        None
    }
}

fn random_bound_target_type(args: &[SqlExpr], arg_types: &[SqlType]) -> SqlType {
    if args.len() == 2
        && let (Some(left), Some(right)) = (
            signed_integer_literal_type(&args[0]),
            signed_integer_literal_type(&args[1]),
        )
    {
        return if matches!(left, SqlTypeKind::Int8) || matches!(right, SqlTypeKind::Int8) {
            SqlType::new(SqlTypeKind::Int8)
        } else {
            SqlType::new(SqlTypeKind::Int4)
        };
    }

    let left_type = arg_types[0];
    let right_type = arg_types[1];
    if matches!(left_type.kind, SqlTypeKind::Numeric)
        || matches!(right_type.kind, SqlTypeKind::Numeric)
    {
        SqlType::new(SqlTypeKind::Numeric)
    } else if matches!(left_type.kind, SqlTypeKind::Int8)
        || matches!(right_type.kind, SqlTypeKind::Int8)
    {
        SqlType::new(SqlTypeKind::Int8)
    } else {
        SqlType::new(SqlTypeKind::Int4)
    }
}

fn pg_typeof_sql_type_name(ty: SqlType, catalog: &dyn CatalogLookup) -> String {
    if ty.type_oid != 0
        && let Some(domain) = catalog.domain_by_type_oid(ty.type_oid)
    {
        return if ty.is_array && (!domain.sql_type.is_array || ty.typrelid == domain.array_oid) {
            format!("{}[]", domain.name)
        } else {
            domain.name
        };
    }
    sql_type_name(ty)
}

pub(super) fn bind_row_to_json_call(
    name: &str,
    args: &[SqlFunctionArg],
    func_variadic: bool,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if args.iter().any(|arg| arg.name.is_some()) {
        return Err(ParseError::FeatureNotSupported(
            "named arguments are not supported for row_to_json".into(),
        ));
    }

    let bound_args = args
        .iter()
        .map(|arg| {
            bind_row_to_json_arg_expr(
                &arg.value,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let actual_types = bound_args
        .iter()
        .map(|(_, sql_type)| *sql_type)
        .collect::<Vec<_>>();
    let resolved =
        resolve_function_call(catalog, name, &actual_types, func_variadic).or_else(|_| {
            let first = actual_types.first().copied();
            let second = actual_types.get(1).copied();
            match (first, second, actual_types.len()) {
                (Some(first), None, 1)
                    if matches!(first.kind, SqlTypeKind::Record | SqlTypeKind::Composite)
                        && !first.is_array =>
                {
                    let mut resolved = resolve_function_call(
                        catalog,
                        name,
                        &[SqlType::record(pgrust_catalog_data::RECORD_TYPE_OID)],
                        func_variadic,
                    )?;
                    resolved.declared_arg_types = vec![first];
                    Ok(resolved)
                }
                (Some(first), Some(second), 2)
                    if matches!(first.kind, SqlTypeKind::Record | SqlTypeKind::Composite)
                        && !first.is_array
                        && second == SqlType::new(SqlTypeKind::Bool) =>
                {
                    let mut resolved = resolve_function_call(
                        catalog,
                        name,
                        &[
                            SqlType::record(pgrust_catalog_data::RECORD_TYPE_OID),
                            SqlType::new(SqlTypeKind::Bool),
                        ],
                        func_variadic,
                    )?;
                    resolved.declared_arg_types = vec![first, second];
                    Ok(resolved)
                }
                _ => Err(ParseError::UnexpectedToken {
                    expected: "supported function",
                    actual: name.into(),
                }),
            }
        })?;
    let coerced_args = bound_args
        .into_iter()
        .zip(resolved.declared_arg_types.iter().copied())
        .map(|((arg, actual_type), declared_type)| {
            coerce_bound_expr(arg, actual_type, declared_type)
        })
        .collect::<Vec<_>>();
    Ok(Expr::resolved_builtin_func(
        BuiltinScalarFunction::RowToJson,
        resolved.proc_oid,
        Some(resolved.result_type),
        resolved.func_variadic,
        coerced_args,
    ))
}

fn bind_row_to_json_arg_expr(
    arg: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(Expr, SqlType), ParseError> {
    match arg {
        SqlExpr::Column(name) => {
            if let Some(resolved) =
                resolve_relation_row_expr_ref_with_outer(scope, outer_scopes, name)
            {
                let relation_oid = resolved.relation_oid;
                let fields = resolved.fields;
                let descriptor = row_to_json_relation_descriptor(relation_oid, &fields, catalog);
                Ok((
                    Expr::Row {
                        descriptor: descriptor.clone(),
                        fields,
                    },
                    descriptor.sql_type(),
                ))
            } else {
                let sql_type = infer_sql_expr_type_with_ctes(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                Ok((
                    bind_expr_with_outer_and_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?,
                    sql_type,
                ))
            }
        }
        _ => {
            let sql_type = infer_sql_expr_type_with_ctes(
                arg,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            Ok((
                bind_expr_with_outer_and_ctes(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                sql_type,
            ))
        }
    }
}

fn row_to_json_relation_descriptor(
    relation_oid: Option<u32>,
    fields: &[(String, Expr)],
    catalog: &dyn CatalogLookup,
) -> RecordDescriptor {
    if let Some((type_oid, typrelid)) = relation_row_type_identity(catalog, relation_oid)
        && let Some(relation) = catalog.lookup_relation_by_oid(typrelid)
    {
        let columns = relation
            .desc
            .columns
            .iter()
            .filter(|column| !column.dropped)
            .collect::<Vec<_>>();
        if columns.len() == fields.len() {
            return RecordDescriptor::named(
                type_oid,
                typrelid,
                -1,
                columns
                    .into_iter()
                    .map(|column| (column.name.clone(), column.sql_type))
                    .collect(),
            );
        }
    }
    assign_anonymous_record_descriptor(
        fields
            .iter()
            .map(|(field_name, expr)| {
                (
                    field_name.clone(),
                    expr_sql_type_hint(expr).unwrap_or(SqlType::new(SqlTypeKind::Text)),
                )
            })
            .collect(),
    )
}

fn bind_json_constructor_arg_expr(
    arg: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(Expr, SqlType), ParseError> {
    match arg {
        SqlExpr::Column(name) => {
            if resolve_column_with_outer(scope, outer_scopes, name, grouped_outer).is_ok() {
                let sql_type = infer_sql_expr_type_with_ctes(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                Ok((
                    bind_expr_with_outer_and_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?,
                    sql_type,
                ))
            } else if let Some(fields) =
                resolve_relation_row_expr_with_outer(scope, outer_scopes, name)
            {
                let descriptor = assign_anonymous_record_descriptor(
                    fields
                        .iter()
                        .map(|(field_name, expr)| {
                            (
                                field_name.clone(),
                                expr_sql_type_hint(expr).unwrap_or(SqlType::new(SqlTypeKind::Text)),
                            )
                        })
                        .collect(),
                );
                Ok((
                    Expr::Row {
                        descriptor: descriptor.clone(),
                        fields,
                    },
                    descriptor.sql_type(),
                ))
            } else {
                let sql_type = infer_sql_expr_type_with_ctes(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                Ok((
                    bind_expr_with_outer_and_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?,
                    sql_type,
                ))
            }
        }
        _ => {
            let sql_type = infer_sql_expr_type_with_ctes(
                arg,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            Ok((
                bind_expr_with_outer_and_ctes(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                sql_type,
            ))
        }
    }
}

pub(super) fn bind_user_defined_scalar_function_call(
    proc_oid: u32,
    funcname: Option<String>,
    result_type: SqlType,
    declared_arg_types: &[SqlType],
    args: &[SqlFunctionArg],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if args.iter().any(|arg| arg.name.is_some()) {
        return Err(ParseError::FeatureNotSupported(
            "named arguments are not supported for user-defined function calls".into(),
        ));
    }
    let bound_args_with_types = args
        .iter()
        .map(|arg| {
            bind_typed_expr_with_outer_and_ctes(
                &arg.value,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    Ok(bind_user_defined_scalar_function_call_from_typed_args(
        proc_oid,
        funcname,
        result_type,
        declared_arg_types,
        bound_args_with_types,
    ))
}

pub(super) fn bind_user_defined_scalar_function_call_from_typed_args(
    proc_oid: u32,
    funcname: Option<String>,
    result_type: SqlType,
    declared_arg_types: &[SqlType],
    bound_args_with_types: Vec<TypedExpr>,
) -> Expr {
    let coerced_args = bound_args_with_types
        .into_iter()
        .zip(declared_arg_types.iter().copied())
        .map(|(arg, declared_type)| coerce_bound_expr(arg.expr, arg.sql_type, declared_type))
        .collect();
    Expr::user_defined_func(proc_oid, funcname, Some(result_type), false, coerced_args)
}

pub fn bind_user_defined_scalar_function_call_from_resolved_typed_args(
    resolved: &ResolvedFunctionCall,
    args: &[SqlExpr],
    bound_args_with_types: Vec<TypedExpr>,
    catalog: &dyn CatalogLookup,
) -> Result<Expr, ParseError> {
    reject_unknown_ordinary_polymorphic_args(resolved, args)?;
    let coerced_args =
        coerce_resolved_user_defined_function_args(resolved, bound_args_with_types, catalog)?;
    // :HACK: Keep SQL-function calls visible to the executor so function
    // EXECUTE privileges are enforced. A later inliner should carry ACL checks.
    let inline_sql_functions = false;
    if inline_sql_functions
        && let Some(inlined) = try_inline_scalar_sql_function(resolved, &coerced_args, catalog)?
    {
        return Ok(inlined);
    }
    Ok(Expr::user_defined_func(
        resolved.proc_oid,
        Some(resolved.proname.clone()),
        Some(resolved.result_type),
        false,
        coerced_args,
    ))
}

fn coerce_resolved_user_defined_function_args(
    resolved: &ResolvedFunctionCall,
    bound_args_with_types: Vec<TypedExpr>,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Expr>, ParseError> {
    let arg_types = bound_args_with_types
        .iter()
        .map(|typed| typed.sql_type)
        .collect::<Vec<_>>();
    let bound_args = bound_args_with_types
        .into_iter()
        .map(|typed| typed.expr)
        .collect::<Vec<_>>();
    let rewritten_args = rewrite_variadic_bound_args(
        bound_args,
        &arg_types,
        &resolved.declared_arg_types,
        resolved.func_variadic,
        resolved.nvargs,
        resolved.vatype_oid,
        catalog,
    )?;
    let coerced_args = if resolved.func_variadic {
        rewritten_args
    } else {
        rewritten_args
            .into_iter()
            .zip(arg_types)
            .zip(resolved.declared_arg_types.iter().copied())
            .map(|((arg, actual_type), declared_type)| {
                coerce_bound_expr(arg, actual_type, declared_type)
            })
            .collect()
    };
    Ok(coerced_args)
}

fn try_inline_scalar_sql_function(
    resolved: &ResolvedFunctionCall,
    coerced_args: &[Expr],
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    let Some(row) = catalog.proc_row_by_oid(resolved.proc_oid) else {
        return Ok(None);
    };
    if !sql_function_is_scalar_inline_candidate(&row) {
        return Ok(None);
    }
    let Some(stmt) = parse_sql_function_select_body(&row.prosrc)? else {
        return Ok(None);
    };
    if !select_body_is_simple_scalar_inline(&stmt) {
        return Ok(None);
    }
    let inline_args = sql_function_inline_args(&row, coerced_args, &resolved.declared_arg_types);
    let empty = empty_scope();
    let bound_targets = with_sql_function_inline_args(inline_args, || {
        bind_select_targets(&stmt.targets, &empty, catalog, &[], None, &[])
    })?;
    let BoundSelectTargets::Plain(targets) = bound_targets;
    if matches!(
        resolved.result_type.kind,
        SqlTypeKind::Composite | SqlTypeKind::Record
    ) && targets.len() != 1
    {
        let Some((descriptor, columns)) =
            sql_function_record_result_descriptor(&row, resolved.result_type, catalog)
        else {
            return Ok(None);
        };
        if targets.len() != columns.len() {
            return Ok(None);
        }
        let fields = targets
            .into_iter()
            .zip(columns.iter())
            .map(|(target, column)| {
                (
                    column.name.clone(),
                    coerce_bound_expr(target.expr, target.sql_type, column.sql_type),
                )
            })
            .collect();
        return Ok(Some(Expr::Row { descriptor, fields }));
    }
    let Ok([target]) = <Vec<TargetEntry> as TryInto<[TargetEntry; 1]>>::try_into(targets) else {
        return Ok(None);
    };
    if matches!(resolved.result_type.kind, SqlTypeKind::Void)
        && !matches!(target.expr, Expr::Func(_))
    {
        return Ok(None);
    }
    Ok(Some(coerce_bound_expr(
        target.expr,
        target.sql_type,
        resolved.result_type,
    )))
}

fn sql_function_is_scalar_inline_candidate(row: &PgProcRow) -> bool {
    row.prolang == PG_LANGUAGE_SQL_OID
        && row.prokind == 'f'
        && !row.proretset
        && !row.prosecdef
        && row.proconfig.is_none()
        && (row.provolatile == 'i' || row.prorettype == pgrust_catalog_data::VOID_TYPE_OID)
}

fn parse_sql_function_select_body(source: &str) -> Result<Option<SelectStatement>, ParseError> {
    let Some(body) = sql_function_select_body_source(source) else {
        return Ok(None);
    };
    match parse_statement(body.as_ref())? {
        Statement::Select(stmt) => Ok(Some(stmt)),
        Statement::Values(values) => Ok(Some(pgrust_parser::wrap_values_as_select(values))),
        _ => Ok(None),
    }
}

fn sql_function_select_body_source(source: &str) -> Option<std::borrow::Cow<'_, str>> {
    let body = source.trim().trim_end_matches(';').trim();
    if body
        .get(.."return".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("return"))
        && body
            .get("return".len()..)
            .and_then(|rest| rest.chars().next())
            .is_none_or(|ch| ch.is_whitespace())
    {
        return Some(std::borrow::Cow::Owned(format!(
            "select {}",
            body["return".len()..].trim()
        )));
    }
    let lower = body.to_ascii_lowercase();
    if lower.starts_with("begin atomic") {
        let without_trailing_semicolon = body.trim_end_matches(';').trim_end();
        let lowered_without_semicolon = without_trailing_semicolon.to_ascii_lowercase();
        let end = if lowered_without_semicolon.ends_with("end") {
            without_trailing_semicolon.len().saturating_sub("end".len())
        } else {
            body.len()
        };
        let inner = body.get("begin atomic".len()..end)?.trim();
        let statements = inner
            .split(';')
            .map(str::trim)
            .filter(|statement| !statement.is_empty() && !statement.eq_ignore_ascii_case("end"))
            .collect::<Vec<_>>();
        if statements.len() == 1 {
            return Some(std::borrow::Cow::Owned(statements[0].to_string()));
        }
        return None;
    }
    sql_function_quoted_body_can_inline(body).then_some(std::borrow::Cow::Borrowed(body))
}

fn sql_function_quoted_body_can_inline(body: &str) -> bool {
    let lower = body.to_ascii_lowercase();
    lower.strip_prefix("select").is_some_and(|rest| {
        rest.chars()
            .next()
            .is_none_or(|ch| ch.is_whitespace() || ch == '(')
    }) || lower.strip_prefix("values").is_some_and(|rest| {
        rest.chars()
            .next()
            .is_none_or(|ch| ch.is_whitespace() || ch == '(')
    })
}

fn select_body_is_simple_scalar_inline(stmt: &SelectStatement) -> bool {
    !stmt.with_recursive
        && stmt.with.is_empty()
        && stmt.from.is_none()
        && stmt.where_clause.is_none()
        && stmt.group_by.is_empty()
        && stmt.having.is_none()
        && stmt.window_clauses.is_empty()
        && stmt.order_by.is_empty()
        && stmt.limit.is_none()
        && stmt.offset.is_none()
        && stmt.locking_clause.is_none()
        && stmt.set_operation.is_none()
}

fn sql_function_record_result_descriptor(
    row: &PgProcRow,
    result_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Option<(RecordDescriptor, Vec<QueryColumn>)> {
    if matches!(result_type.kind, SqlTypeKind::Composite) && result_type.typrelid != 0 {
        let relation = catalog.lookup_relation_by_oid(result_type.typrelid)?;
        let columns = relation
            .desc
            .columns
            .into_iter()
            .filter(|column| !column.dropped)
            .map(|column| QueryColumn {
                name: column.name,
                sql_type: column.sql_type,
                wire_type_oid: None,
            })
            .collect::<Vec<_>>();
        let descriptor = RecordDescriptor::named(
            row.prorettype,
            result_type.typrelid,
            result_type.typmod,
            columns
                .iter()
                .map(|column| (column.name.clone(), column.sql_type))
                .collect(),
        );
        return Some((descriptor, columns));
    }
    if matches!(result_type.kind, SqlTypeKind::Record) {
        return None;
    }
    None
}

fn sql_function_inline_args(
    row: &PgProcRow,
    coerced_args: &[Expr],
    declared_arg_types: &[SqlType],
) -> Vec<SqlFunctionInlineArg> {
    let names = sql_function_input_arg_names(row);
    coerced_args
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, expr)| SqlFunctionInlineArg {
            function_name: Some(row.proname.clone()),
            name: names.get(index).cloned().flatten(),
            sql_type: declared_arg_types
                .get(index)
                .copied()
                .or_else(|| expr_sql_type_hint(&expr))
                .unwrap_or(SqlType::new(SqlTypeKind::Text)),
            expr,
        })
        .collect()
}

fn sql_function_input_arg_names(row: &PgProcRow) -> Vec<Option<String>> {
    let names = row.proargnames.as_deref().unwrap_or(&[]);
    let Some(modes) = row.proargmodes.as_ref() else {
        return (0..row.pronargs.max(0) as usize)
            .map(|index| names.get(index).filter(|name| !name.is_empty()).cloned())
            .collect();
    };
    modes
        .iter()
        .copied()
        .enumerate()
        .filter_map(|(index, mode)| {
            matches!(mode, b'i' | b'b' | b'v')
                .then(|| names.get(index).filter(|name| !name.is_empty()).cloned())
        })
        .collect()
}

fn reject_unknown_ordinary_polymorphic_args(
    resolved: &ResolvedFunctionCall,
    args: &[SqlExpr],
) -> Result<(), ParseError> {
    for (declared_oid, arg) in resolved.declared_arg_oids.iter().copied().zip(args.iter()) {
        if !matches!(
            declared_oid,
            ANYELEMENTOID
                | ANYARRAYOID
                | ANYNONARRAYOID
                | ANYENUMOID
                | ANYRANGEOID
                | ANYMULTIRANGEOID
        ) {
            continue;
        }
        if matches!(
            arg,
            SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
        ) {
            return Err(ParseError::DetailedError {
                message: "could not determine polymorphic type because input has type unknown"
                    .into(),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
    }
    Ok(())
}

pub(super) fn bind_resolved_user_defined_scalar_function_call(
    resolved: &ResolvedFunctionCall,
    args: &[SqlExpr],
    display_args: Option<&[SqlFunctionArg]>,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let bound_args = args
        .iter()
        .map(|arg| {
            bind_typed_expr_with_outer_and_ctes(
                arg,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    let mut expr = bind_user_defined_scalar_function_call_from_resolved_typed_args(
        resolved, args, bound_args, catalog,
    )?;
    if let Some(display_args) = display_args
        && display_args.iter().any(|arg| arg.name.is_some())
    {
        let bound_display_args = display_args
            .iter()
            .map(|arg| {
                Ok(pgrust_nodes::primnodes::FuncCallDisplayArg {
                    name: arg.name.clone(),
                    expr: bind_expr_with_outer_and_ctes(
                        &arg.value,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, ParseError>>()?;
        if let Expr::Func(func) = &mut expr {
            func.display_args = Some(bound_display_args);
        }
    }
    Ok(expr)
}

pub(super) fn bind_resolved_scalar_function_call(
    resolved: &ResolvedFunctionCall,
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if let Some(func) = resolved.scalar_impl {
        return bind_scalar_function_call(
            func,
            resolved.proc_oid,
            Some(resolved.result_type),
            resolved.func_variadic,
            resolved.nvargs,
            resolved.vatype_oid,
            &resolved.declared_arg_types,
            args,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
    }

    bind_resolved_user_defined_scalar_function_call(
        resolved,
        args,
        None,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )
}

pub(super) fn bind_scalar_function_call(
    func: BuiltinScalarFunction,
    func_oid: u32,
    result_type: Option<SqlType>,
    func_variadic: bool,
    nvargs: usize,
    vatype_oid: u32,
    declared_arg_types: &[SqlType],
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let bound_args_with_types = if matches!(
        func,
        BuiltinScalarFunction::JsonBuildArray
            | BuiltinScalarFunction::JsonBuildObject
            | BuiltinScalarFunction::JsonbBuildArray
            | BuiltinScalarFunction::JsonbBuildObject
    ) {
        args.iter()
            .map(|arg| {
                bind_json_constructor_arg_expr(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        args.iter()
            .map(|arg| {
                bind_typed_expr_with_outer_and_ctes(
                    arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
                .map(|typed| (typed.expr, typed.sql_type))
            })
            .collect::<Result<Vec<_>, ParseError>>()?
    };
    bind_scalar_function_call_from_bound_args(
        func,
        func_oid,
        result_type,
        func_variadic,
        nvargs,
        vatype_oid,
        declared_arg_types,
        args,
        bound_args_with_types,
        catalog,
        scope,
        outer_scopes,
        grouped_outer,
        ctes,
    )
}

pub(super) fn bind_scalar_function_call_from_typed_args(
    func: BuiltinScalarFunction,
    func_oid: u32,
    result_type: Option<SqlType>,
    func_variadic: bool,
    nvargs: usize,
    vatype_oid: u32,
    declared_arg_types: &[SqlType],
    args: &[SqlExpr],
    bound_args_with_types: Vec<TypedExpr>,
    catalog: &dyn CatalogLookup,
    scope: &BoundScope,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let bound_args_with_types = bound_args_with_types
        .into_iter()
        .map(|typed| (typed.expr, typed.sql_type))
        .collect();
    bind_scalar_function_call_from_bound_args(
        func,
        func_oid,
        result_type,
        func_variadic,
        nvargs,
        vatype_oid,
        declared_arg_types,
        args,
        bound_args_with_types,
        catalog,
        scope,
        outer_scopes,
        grouped_outer,
        ctes,
    )
}

fn bind_scalar_function_call_from_bound_args(
    func: BuiltinScalarFunction,
    func_oid: u32,
    result_type: Option<SqlType>,
    func_variadic: bool,
    nvargs: usize,
    vatype_oid: u32,
    declared_arg_types: &[SqlType],
    args: &[SqlExpr],
    bound_args_with_types: Vec<(Expr, SqlType)>,
    catalog: &dyn CatalogLookup,
    scope: &BoundScope,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let arg_types = bound_args_with_types
        .iter()
        .map(|(_, sql_type)| *sql_type)
        .collect::<Vec<_>>();
    let bound_args = bound_args_with_types
        .into_iter()
        .map(|(bound, _)| bound)
        .collect::<Vec<_>>();
    let bound_args = if matches!(func, BuiltinScalarFunction::SatisfiesHashPartition) {
        preserve_satisfies_hash_partition_arg_types(bound_args, &arg_types)
    } else {
        bound_args
    };
    let rewritten_bound_args = rewrite_variadic_bound_args(
        bound_args.clone(),
        &arg_types,
        declared_arg_types,
        func_variadic,
        nvargs,
        vatype_oid,
        catalog,
    )?;
    let build_func = |funcvariadic: bool, args: Vec<Expr>| {
        let mut expr = Expr::resolved_builtin_func(func, func_oid, result_type, funcvariadic, args);
        if matches!(func, BuiltinScalarFunction::TextStartsWith)
            && let Expr::Func(func_expr) = &mut expr
        {
            func_expr.funcname = Some("starts_with".into());
        }
        expr
    };
    if matches!(
        func,
        BuiltinScalarFunction::EnumFirst
            | BuiltinScalarFunction::EnumLast
            | BuiltinScalarFunction::EnumRange
    ) {
        let enum_type = result_type
            .map(|ty| if ty.is_array { ty.element_type() } else { ty })
            .filter(|ty| matches!(ty.kind, SqlTypeKind::Enum) && ty.type_oid != 0)
            .or_else(|| {
                arg_types
                    .iter()
                    .copied()
                    .find(|ty| matches!(ty.kind, SqlTypeKind::Enum) && ty.type_oid != 0)
            })
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "concrete enum argument",
                actual: format!("{func:?}({} args)", args.len()),
            })?;
        let result_type = if matches!(func, BuiltinScalarFunction::EnumRange) {
            SqlType::array_of(enum_type)
        } else {
            enum_type
        };
        let coerced = bound_args
            .iter()
            .zip(arg_types.iter().copied())
            .map(|(arg, actual_type)| coerce_bound_expr(arg.clone(), actual_type, enum_type))
            .collect();
        return Ok(Expr::resolved_builtin_func(
            func,
            func_oid,
            Some(result_type),
            func_variadic,
            coerced,
        ));
    }
    match func {
        BuiltinScalarFunction::Random | BuiltinScalarFunction::RandomNormal => {
            if bound_args.is_empty() {
                return Ok(build_func(false, bound_args));
            }

            let target_types = if matches!(func, BuiltinScalarFunction::RandomNormal) {
                vec![SqlType::new(SqlTypeKind::Float8); bound_args.len()]
            } else if bound_args.len() == 2 {
                let target = random_bound_target_type(args, &arg_types);
                vec![target; 2]
            } else if declared_arg_types.len() == bound_args.len() {
                declared_arg_types.to_vec()
            } else {
                arg_types.clone()
            };

            let coerced = bound_args
                .into_iter()
                .zip(arg_types)
                .zip(target_types)
                .map(|((arg, actual_type), declared_type)| {
                    coerce_bound_expr(arg, actual_type, declared_type)
                })
                .collect();
            Ok(build_func(false, coerced))
        }
        BuiltinScalarFunction::CashLarger | BuiltinScalarFunction::CashSmaller => {
            let money = SqlType::new(SqlTypeKind::Money);
            let coerced = bound_args
                .into_iter()
                .zip(arg_types)
                .map(|(arg, ty)| coerce_bound_expr(arg, ty, money))
                .collect();
            Ok(build_func(false, coerced))
        }
        BuiltinScalarFunction::CashWords => Ok(build_func(
            false,
            vec![coerce_bound_expr(
                bound_args[0].clone(),
                arg_types[0],
                SqlType::new(SqlTypeKind::Money),
            )],
        )),
        BuiltinScalarFunction::Abs => {
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let arg_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_arg_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            let target_type = match arg_type.element_type().kind {
                SqlTypeKind::Int2 => SqlType::new(SqlTypeKind::Int2),
                SqlTypeKind::Int4 => SqlType::new(SqlTypeKind::Int4),
                SqlTypeKind::Int8 => SqlType::new(SqlTypeKind::Int8),
                SqlTypeKind::Float4 => SqlType::new(SqlTypeKind::Float4),
                SqlTypeKind::Float8 => SqlType::new(SqlTypeKind::Float8),
                _ => SqlType::new(SqlTypeKind::Numeric),
            };
            Ok(Expr::resolved_builtin_func(
                func,
                func_oid,
                Some(target_type),
                func_variadic,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    target_type,
                )],
            ))
        }
        BuiltinScalarFunction::UuidExtractVersion | BuiltinScalarFunction::UuidExtractTimestamp => {
            Ok(build_func(
                false,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_types[0],
                    SqlType::new(SqlTypeKind::Uuid),
                )],
            ))
        }
        BuiltinScalarFunction::DatePart | BuiltinScalarFunction::Extract => Ok(build_func(
            false,
            vec![
                coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_types[0],
                    SqlType::new(SqlTypeKind::Text),
                ),
                bound_args[1].clone(),
            ],
        )),
        BuiltinScalarFunction::DateTrunc => {
            let target_type = match arg_types[1].kind {
                SqlTypeKind::Date => SqlType::new(SqlTypeKind::Date),
                SqlTypeKind::Interval => SqlType::new(SqlTypeKind::Interval),
                SqlTypeKind::Timestamp => SqlType::new(SqlTypeKind::Timestamp),
                SqlTypeKind::TimestampTz => SqlType::new(SqlTypeKind::TimestampTz),
                _ => arg_types[1],
            };
            let mut args = vec![
                coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_types[0],
                    SqlType::new(SqlTypeKind::Text),
                ),
                coerce_bound_expr(bound_args[1].clone(), arg_types[1], target_type),
            ];
            if bound_args.len() == 3 {
                args.push(coerce_bound_expr(
                    bound_args[2].clone(),
                    arg_types[2],
                    SqlType::new(SqlTypeKind::Text),
                ));
            }
            Ok(build_func(false, args))
        }
        BuiltinScalarFunction::DateBin => {
            let target_type = match arg_types[1].kind {
                SqlTypeKind::TimestampTz => SqlType::new(SqlTypeKind::TimestampTz),
                _ => SqlType::new(SqlTypeKind::Timestamp),
            };
            Ok(Expr::resolved_builtin_func(
                func,
                func_oid,
                Some(target_type),
                false,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        arg_types[0],
                        SqlType::new(SqlTypeKind::Interval),
                    ),
                    coerce_bound_expr(bound_args[1].clone(), arg_types[1], target_type),
                    coerce_bound_expr(bound_args[2].clone(), arg_types[2], target_type),
                ],
            ))
        }
        BuiltinScalarFunction::DateAdd | BuiltinScalarFunction::DateSubtract => {
            let mut args = vec![
                coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_types[0],
                    SqlType::new(SqlTypeKind::TimestampTz),
                ),
                coerce_bound_expr(
                    bound_args[1].clone(),
                    arg_types[1],
                    SqlType::new(SqlTypeKind::Interval),
                ),
            ];
            if bound_args.len() == 3 {
                args.push(coerce_bound_expr(
                    bound_args[2].clone(),
                    arg_types[2],
                    SqlType::new(SqlTypeKind::Text),
                ));
            }
            Ok(Expr::resolved_builtin_func(
                func,
                func_oid,
                Some(SqlType::new(SqlTypeKind::TimestampTz)),
                false,
                args,
            ))
        }
        BuiltinScalarFunction::PgSleep => {
            let target_type = declared_arg_types
                .first()
                .copied()
                .filter(|ty| matches!(ty.kind, SqlTypeKind::Interval))
                .unwrap_or_else(|| SqlType::new(SqlTypeKind::Float8));
            Ok(build_func(
                false,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_types[0],
                    target_type,
                )],
            ))
        }
        BuiltinScalarFunction::LoCreate
        | BuiltinScalarFunction::LoUnlink
        | BuiltinScalarFunction::LoOpen
        | BuiltinScalarFunction::LoClose
        | BuiltinScalarFunction::LoRead
        | BuiltinScalarFunction::LoWrite
        | BuiltinScalarFunction::LoLseek
        | BuiltinScalarFunction::LoLseek64
        | BuiltinScalarFunction::LoTell
        | BuiltinScalarFunction::LoTell64
        | BuiltinScalarFunction::LoTruncate
        | BuiltinScalarFunction::LoTruncate64
        | BuiltinScalarFunction::LoCreat
        | BuiltinScalarFunction::LoFromBytea
        | BuiltinScalarFunction::LoGet
        | BuiltinScalarFunction::LoPut
        | BuiltinScalarFunction::LoImport
        | BuiltinScalarFunction::LoExport => {
            let target_types = if declared_arg_types.len() == bound_args.len() {
                declared_arg_types.to_vec()
            } else {
                arg_types.clone()
            };
            let coerced = bound_args
                .into_iter()
                .zip(arg_types)
                .zip(target_types)
                .map(|((arg, actual_type), declared_type)| {
                    coerce_bound_expr(arg, actual_type, declared_type)
                })
                .collect();
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::JustifyDays
        | BuiltinScalarFunction::JustifyHours
        | BuiltinScalarFunction::JustifyInterval => Ok(build_func(
            false,
            vec![coerce_bound_expr(
                bound_args[0].clone(),
                arg_types[0],
                SqlType::new(SqlTypeKind::Interval),
            )],
        )),
        BuiltinScalarFunction::Timezone => {
            let source_index = if arg_types.len() == 1 { 0 } else { 1 };
            let source_type = arg_types[source_index];
            let source_is_time = matches!(source_type.kind, SqlTypeKind::Time);
            let source_is_timetz = matches!(source_type.kind, SqlTypeKind::TimeTz);
            let source_is_timestamptz = !source_is_timetz
                && !source_is_time
                && (matches!(source_type.kind, SqlTypeKind::TimestampTz)
                    || matches!(
                        &args[source_index],
                        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
                    ));
            let result_type = if source_is_timetz {
                SqlType::new(SqlTypeKind::TimeTz)
            } else if source_is_time {
                SqlType::new(SqlTypeKind::TimeTz)
            } else if source_is_timestamptz {
                SqlType::new(SqlTypeKind::Timestamp)
            } else {
                SqlType::new(SqlTypeKind::TimestampTz)
            };
            let source_target = if source_is_timetz {
                source_type
            } else if source_is_time {
                source_type
            } else if source_is_timestamptz {
                SqlType::new(SqlTypeKind::TimestampTz)
            } else {
                SqlType::new(SqlTypeKind::Timestamp)
            };
            let mut rewritten_args = Vec::new();
            if bound_args.len() == 2 {
                let zone_target = if matches!(
                    arg_types[0].kind,
                    SqlTypeKind::Text
                        | SqlTypeKind::Name
                        | SqlTypeKind::Char
                        | SqlTypeKind::Varchar
                ) {
                    SqlType::new(SqlTypeKind::Text)
                } else {
                    SqlType::new(SqlTypeKind::Interval)
                };
                rewritten_args.push(coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_types[0],
                    zone_target,
                ));
            }
            rewritten_args.push(coerce_bound_expr(
                bound_args[source_index].clone(),
                source_type,
                source_target,
            ));
            Ok(Expr::resolved_builtin_func(
                func,
                func_oid,
                Some(result_type),
                false,
                rewritten_args,
            ))
        }
        BuiltinScalarFunction::IsFinite => Ok(build_func(false, bound_args)),
        BuiltinScalarFunction::PgColumnSize | BuiltinScalarFunction::PgRelationSize => {
            Ok(build_func(false, bound_args))
        }
        BuiltinScalarFunction::MakeInterval => {
            let mut args = bound_args;
            let mut types = arg_types;
            let defaults = [
                (
                    Expr::Const(Value::Int32(0)),
                    SqlType::new(SqlTypeKind::Int4),
                ),
                (
                    Expr::Const(Value::Int32(0)),
                    SqlType::new(SqlTypeKind::Int4),
                ),
                (
                    Expr::Const(Value::Int32(0)),
                    SqlType::new(SqlTypeKind::Int4),
                ),
                (
                    Expr::Const(Value::Int32(0)),
                    SqlType::new(SqlTypeKind::Int4),
                ),
                (
                    Expr::Const(Value::Int32(0)),
                    SqlType::new(SqlTypeKind::Int4),
                ),
                (
                    Expr::Const(Value::Int32(0)),
                    SqlType::new(SqlTypeKind::Int4),
                ),
                (
                    Expr::Const(Value::Float64(0.0)),
                    SqlType::new(SqlTypeKind::Float8),
                ),
            ];
            for (default_expr, default_type) in defaults.iter().skip(args.len()) {
                args.push(default_expr.clone());
                types.push(*default_type);
            }
            Ok(build_func(
                false,
                args.into_iter()
                    .zip(types)
                    .enumerate()
                    .map(|(idx, (arg, ty))| {
                        let target = if idx == 6 {
                            SqlType::new(SqlTypeKind::Float8)
                        } else {
                            SqlType::new(SqlTypeKind::Int4)
                        };
                        coerce_bound_expr(arg, ty, target)
                    })
                    .collect(),
            ))
        }
        BuiltinScalarFunction::MakeDate | BuiltinScalarFunction::MakeTime => {
            let target_types = if func == BuiltinScalarFunction::MakeDate {
                [
                    SqlType::new(SqlTypeKind::Int4),
                    SqlType::new(SqlTypeKind::Int4),
                    SqlType::new(SqlTypeKind::Int4),
                ]
            } else {
                [
                    SqlType::new(SqlTypeKind::Int4),
                    SqlType::new(SqlTypeKind::Int4),
                    SqlType::new(SqlTypeKind::Float8),
                ]
            };
            Ok(build_func(
                false,
                arg_types
                    .into_iter()
                    .zip(bound_args)
                    .zip(target_types)
                    .map(|((ty, arg), target)| coerce_bound_expr(arg, ty, target))
                    .collect(),
            ))
        }
        BuiltinScalarFunction::MakeTimestamp => Ok(build_func(
            false,
            arg_types
                .into_iter()
                .zip(bound_args)
                .enumerate()
                .map(|(idx, (ty, arg))| {
                    let target = if idx == 5 {
                        SqlType::new(SqlTypeKind::Float8)
                    } else {
                        SqlType::new(SqlTypeKind::Int4)
                    };
                    coerce_bound_expr(arg, ty, target)
                })
                .collect(),
        )),
        BuiltinScalarFunction::MakeTimestampTz => {
            let mut args = Vec::new();
            for idx in 0..5 {
                args.push(coerce_bound_expr(
                    bound_args[idx].clone(),
                    arg_types[idx],
                    SqlType::new(SqlTypeKind::Int4),
                ));
            }
            args.push(coerce_bound_expr(
                bound_args[5].clone(),
                arg_types[5],
                SqlType::new(SqlTypeKind::Float8),
            ));
            if bound_args.len() == 7 {
                args.push(coerce_bound_expr(
                    bound_args[6].clone(),
                    arg_types[6],
                    SqlType::new(SqlTypeKind::Text),
                ));
            }
            Ok(build_func(false, args))
        }
        BuiltinScalarFunction::TimestampTzConstructor => {
            let second_type = match arg_types.get(1).map(|ty| ty.kind) {
                Some(SqlTypeKind::TimeTz) => SqlType::new(SqlTypeKind::TimeTz),
                _ => SqlType::new(SqlTypeKind::Time),
            };
            Ok(build_func(
                false,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        arg_types[0],
                        SqlType::new(SqlTypeKind::Date),
                    ),
                    coerce_bound_expr(bound_args[1].clone(), arg_types[1], second_type),
                ],
            ))
        }
        BuiltinScalarFunction::Age => Ok(build_func(false, bound_args)),
        BuiltinScalarFunction::IntervalHash => Ok(build_func(
            false,
            vec![coerce_bound_expr(
                bound_args[0].clone(),
                arg_types[0],
                SqlType::new(SqlTypeKind::Interval),
            )],
        )),
        BuiltinScalarFunction::ToTsVector => Ok(build_func(
            false,
            args.iter()
                .enumerate()
                .map(|(idx, arg)| {
                    let ty = infer_sql_expr_type_with_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    let target = if idx == args.len().saturating_sub(1)
                        && matches!(ty.kind, SqlTypeKind::Json | SqlTypeKind::Jsonb)
                    {
                        ty
                    } else {
                        SqlType::new(SqlTypeKind::Text)
                    };
                    coerce_bound_expr(bound_args[idx].clone(), ty, target)
                })
                .collect(),
        )),
        BuiltinScalarFunction::JsonToTsVector => Ok(build_func(
            false,
            args.iter()
                .enumerate()
                .map(|(idx, arg)| {
                    let ty = infer_sql_expr_type_with_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    let target = match (args.len(), idx) {
                        (2, 0) | (3, 1) => SqlType::new(SqlTypeKind::Json),
                        (2, 1) | (3, 2) => SqlType::new(SqlTypeKind::Jsonb),
                        _ => SqlType::new(SqlTypeKind::Text),
                    };
                    coerce_bound_expr(bound_args[idx].clone(), ty, target)
                })
                .collect(),
        )),
        BuiltinScalarFunction::JsonbToTsVector => Ok(build_func(
            false,
            args.iter()
                .enumerate()
                .map(|(idx, arg)| {
                    let ty = infer_sql_expr_type_with_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    let jsonb_arg = (args.len() == 2 && idx <= 1) || (args.len() == 3 && idx >= 1);
                    let target = if jsonb_arg {
                        SqlType::new(SqlTypeKind::Jsonb)
                    } else {
                        SqlType::new(SqlTypeKind::Text)
                    };
                    coerce_bound_expr(bound_args[idx].clone(), ty, target)
                })
                .collect(),
        )),
        BuiltinScalarFunction::ToTsQuery
        | BuiltinScalarFunction::PlainToTsQuery
        | BuiltinScalarFunction::PhraseToTsQuery
        | BuiltinScalarFunction::WebSearchToTsQuery
        | BuiltinScalarFunction::TsLexize => Ok(build_func(
            false,
            args.iter()
                .enumerate()
                .map(|(idx, arg)| {
                    let ty = infer_sql_expr_type_with_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    coerce_bound_expr(bound_args[idx].clone(), ty, SqlType::new(SqlTypeKind::Text))
                })
                .collect(),
        )),
        BuiltinScalarFunction::TsHeadline => {
            let document_index = match bound_args.len() {
                2 => 0,
                3 if matches!(
                    arg_types.get(1).map(|ty| ty.kind),
                    Some(SqlTypeKind::TsQuery)
                ) =>
                {
                    0
                }
                3 | 4 => 1,
                _ => unreachable!("ts_headline arity validated earlier"),
            };
            let document_target = match arg_types.get(document_index).map(|ty| ty.kind) {
                Some(SqlTypeKind::Json) => SqlType::new(SqlTypeKind::Json),
                Some(SqlTypeKind::Jsonb) => SqlType::new(SqlTypeKind::Jsonb),
                _ => SqlType::new(SqlTypeKind::Text),
            };
            let targets = match bound_args.len() {
                2 => vec![document_target, SqlType::new(SqlTypeKind::TsQuery)],
                3 if matches!(
                    arg_types.get(1).map(|ty| ty.kind),
                    Some(SqlTypeKind::TsQuery)
                ) =>
                {
                    vec![
                        document_target,
                        SqlType::new(SqlTypeKind::TsQuery),
                        SqlType::new(SqlTypeKind::Text),
                    ]
                }
                3 => vec![
                    SqlType::new(SqlTypeKind::Text),
                    document_target,
                    SqlType::new(SqlTypeKind::TsQuery),
                ],
                4 => vec![
                    SqlType::new(SqlTypeKind::Text),
                    document_target,
                    SqlType::new(SqlTypeKind::TsQuery),
                    SqlType::new(SqlTypeKind::Text),
                ],
                _ => unreachable!("ts_headline arity validated earlier"),
            };
            Ok(build_func(
                false,
                bound_args
                    .iter()
                    .cloned()
                    .zip(arg_types)
                    .zip(targets)
                    .map(|((arg, actual), target)| coerce_bound_expr(arg, actual, target))
                    .collect(),
            ))
        }
        BuiltinScalarFunction::TsVectorIn | BuiltinScalarFunction::TsQueryIn => {
            // :HACK: pgrust uses text values for SQL cstring input shims.
            let mut args = vec![coerce_bound_expr(
                bound_args[0].clone(),
                arg_types[0],
                SqlType::new(SqlTypeKind::Text),
            )];
            if bound_args.len() == 3 {
                args.push(coerce_bound_expr(
                    bound_args[1].clone(),
                    arg_types[1],
                    SqlType::new(SqlTypeKind::Oid),
                ));
                args.push(coerce_bound_expr(
                    bound_args[2].clone(),
                    arg_types[2],
                    SqlType::new(SqlTypeKind::Int4),
                ));
            }
            Ok(build_func(false, args))
        }
        BuiltinScalarFunction::TsVectorOut => Ok(build_func(
            false,
            vec![coerce_bound_expr(
                bound_args[0].clone(),
                arg_types[0],
                SqlType::new(SqlTypeKind::TsVector),
            )],
        )),
        BuiltinScalarFunction::TsQueryOut | BuiltinScalarFunction::TsQueryNumnode => {
            Ok(build_func(
                false,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_types[0],
                    SqlType::new(SqlTypeKind::TsQuery),
                )],
            ))
        }
        BuiltinScalarFunction::TsQueryPhrase => {
            let mut coerced = vec![
                coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_types[0],
                    SqlType::new(SqlTypeKind::TsQuery),
                ),
                coerce_bound_expr(
                    bound_args[1].clone(),
                    arg_types[1],
                    SqlType::new(SqlTypeKind::TsQuery),
                ),
            ];
            if bound_args.len() == 3 {
                coerced.push(coerce_bound_expr(
                    bound_args[2].clone(),
                    arg_types[2],
                    SqlType::new(SqlTypeKind::Int4),
                ));
            }
            Ok(build_func(false, coerced))
        }
        BuiltinScalarFunction::TsQueryContains | BuiltinScalarFunction::TsQueryContainedBy => {
            Ok(build_func(
                false,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        arg_types[0],
                        SqlType::new(SqlTypeKind::TsQuery),
                    ),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        arg_types[1],
                        SqlType::new(SqlTypeKind::TsQuery),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::TsRewrite => {
            let mut coerced = vec![coerce_bound_expr(
                bound_args[0].clone(),
                arg_types[0],
                SqlType::new(SqlTypeKind::TsQuery),
            )];
            if bound_args.len() == 3 {
                coerced.push(coerce_bound_expr(
                    bound_args[1].clone(),
                    arg_types[1],
                    SqlType::new(SqlTypeKind::TsQuery),
                ));
                coerced.push(coerce_bound_expr(
                    bound_args[2].clone(),
                    arg_types[2],
                    SqlType::new(SqlTypeKind::TsQuery),
                ));
            } else {
                coerced.push(coerce_bound_expr(
                    bound_args[1].clone(),
                    arg_types[1],
                    SqlType::new(SqlTypeKind::Text),
                ));
            }
            Ok(build_func(false, coerced))
        }
        BuiltinScalarFunction::TsVectorStrip | BuiltinScalarFunction::TsVectorToArray => {
            Ok(build_func(
                false,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_types[0],
                    SqlType::new(SqlTypeKind::TsVector),
                )],
            ))
        }
        BuiltinScalarFunction::ArrayToTsVector => Ok(build_func(
            false,
            vec![coerce_bound_expr(
                bound_args[0].clone(),
                arg_types[0],
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
            )],
        )),
        BuiltinScalarFunction::TsVectorDelete => {
            let second_target = if arg_types
                .get(1)
                .is_some_and(|ty| ty.is_array || matches!(ty.kind, SqlTypeKind::Text))
                && arg_types.get(1).is_some_and(|ty| ty.is_array)
            {
                SqlType::array_of(SqlType::new(SqlTypeKind::Text))
            } else {
                SqlType::new(SqlTypeKind::Text)
            };
            Ok(build_func(
                false,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        arg_types[0],
                        SqlType::new(SqlTypeKind::TsVector),
                    ),
                    coerce_bound_expr(bound_args[1].clone(), arg_types[1], second_target),
                ],
            ))
        }
        BuiltinScalarFunction::TsVectorSetWeight => {
            let mut coerced = vec![
                coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_types[0],
                    SqlType::new(SqlTypeKind::TsVector),
                ),
                coerce_bound_expr(
                    bound_args[1].clone(),
                    arg_types[1],
                    SqlType::new(SqlTypeKind::InternalChar),
                ),
            ];
            if bound_args.len() == 3 {
                coerced.push(coerce_bound_expr(
                    bound_args[2].clone(),
                    arg_types[2],
                    SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                ));
            }
            Ok(build_func(false, coerced))
        }
        BuiltinScalarFunction::TsVectorFilter => Ok(build_func(
            false,
            vec![
                coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_types[0],
                    SqlType::new(SqlTypeKind::TsVector),
                ),
                coerce_bound_expr(
                    bound_args[1].clone(),
                    arg_types[1],
                    SqlType::array_of(SqlType::new(SqlTypeKind::InternalChar)),
                ),
            ],
        )),
        BuiltinScalarFunction::TsRank | BuiltinScalarFunction::TsRankCd => {
            let mut coerced = Vec::with_capacity(bound_args.len());
            let mut offset = 0usize;
            if bound_args.len() == 3 && arg_types[0].is_array || bound_args.len() == 4 {
                coerced.push(coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_types[0],
                    SqlType::array_of(SqlType::new(SqlTypeKind::Float4)),
                ));
                offset = 1;
            }
            coerced.push(coerce_bound_expr(
                bound_args[offset].clone(),
                arg_types[offset],
                SqlType::new(SqlTypeKind::TsVector),
            ));
            coerced.push(coerce_bound_expr(
                bound_args[offset + 1].clone(),
                arg_types[offset + 1],
                SqlType::new(SqlTypeKind::TsQuery),
            ));
            if bound_args.len() > offset + 2 {
                coerced.push(coerce_bound_expr(
                    bound_args[offset + 2].clone(),
                    arg_types[offset + 2],
                    SqlType::new(SqlTypeKind::Int4),
                ));
            }
            Ok(build_func(false, coerced))
        }
        BuiltinScalarFunction::Left
        | BuiltinScalarFunction::Right
        | BuiltinScalarFunction::Repeat => {
            let left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
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
            Ok(build_func(
                func_variadic,
                vec![
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
            ))
        }
        BuiltinScalarFunction::Concat => Ok(build_func(func_variadic, bound_args)),
        BuiltinScalarFunction::ConcatWs => {
            let separator_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let mut lowered = Vec::with_capacity(bound_args.len());
            lowered.push(coerce_bound_expr(
                bound_args[0].clone(),
                separator_type,
                SqlType::new(SqlTypeKind::Text),
            ));
            lowered.extend(bound_args.iter().skip(1).cloned());
            Ok(build_func(func_variadic, lowered))
        }
        BuiltinScalarFunction::Format => {
            let format_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let mut lowered = Vec::with_capacity(bound_args.len());
            lowered.push(coerce_bound_expr(
                bound_args[0].clone(),
                format_type,
                SqlType::new(SqlTypeKind::Text),
            ));
            lowered.extend(bound_args.iter().skip(1).cloned());
            Ok(build_func(func_variadic, lowered))
        }
        BuiltinScalarFunction::Length => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !matches!(arg_type.kind, SqlTypeKind::TsVector)
                && !is_bit_string_type(arg_type)
                && arg_type.kind != SqlTypeKind::Bytea
                && !should_use_text_concat(&args[0], arg_type, &args[0], arg_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "text, bytea, bit, or tsvector argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            let arg = if matches!(arg_type.kind, SqlTypeKind::Char) && !arg_type.is_array {
                coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Text),
                )
            } else {
                bound_args[0].clone()
            };
            Ok(build_func(func_variadic, vec![arg]))
        }
        BuiltinScalarFunction::OctetLength => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !is_bit_string_type(arg_type)
                && arg_type.kind != SqlTypeKind::Bytea
                && !should_use_text_concat(&args[0], arg_type, &args[0], arg_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "text, bytea, or bit argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            let arg = if matches!(arg_type.kind, SqlTypeKind::Char) && !arg_type.is_array {
                coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Text),
                )
            } else {
                bound_args[0].clone()
            };
            Ok(build_func(func_variadic, vec![arg]))
        }
        BuiltinScalarFunction::Position => {
            let left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if is_bit_string_type(left_type) && is_bit_string_type(right_type) {
                let common = resolve_common_scalar_type(left_type, right_type)
                    .unwrap_or(SqlType::new(SqlTypeKind::VarBit));
                return Ok(build_func(
                    func_variadic,
                    vec![
                        coerce_bound_expr(bound_args[0].clone(), left_type, common),
                        coerce_bound_expr(bound_args[1].clone(), right_type, common),
                    ],
                ));
            }
            if left_type.kind == SqlTypeKind::Bytea && right_type.kind == SqlTypeKind::Bytea {
                return Ok(build_func(
                    func_variadic,
                    vec![bound_args[0].clone(), bound_args[1].clone()],
                ));
            }
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        left_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        right_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::Strpos => {
            let left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        left_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        right_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::Substring => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let start_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if is_bit_string_type(value_type) {
                if !is_integer_family(start_type) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "substring(bit, int4[, int4])",
                        actual: format!(
                            "{func:?}({}, {})",
                            sql_type_name(value_type),
                            sql_type_name(start_type)
                        ),
                    });
                }
                let mut coerced = vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        start_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ];
                if let Some(len_arg) = args.get(2) {
                    let len_type = infer_sql_expr_type_with_ctes(
                        len_arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    if !is_integer_family(len_type) {
                        return Err(ParseError::UnexpectedToken {
                            expected: "integer length argument",
                            actual: sql_type_name(len_type),
                        });
                    }
                    coerced.push(coerce_bound_expr(
                        bound_args[2].clone(),
                        len_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ));
                }
                return Ok(build_func(func_variadic, coerced));
            }
            if value_type.kind == SqlTypeKind::Bytea {
                if !is_integer_family(start_type) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "substring(bytea, int4[, int4])",
                        actual: format!(
                            "{func:?}({}, {})",
                            sql_type_name(value_type),
                            sql_type_name(start_type)
                        ),
                    });
                }
                let mut coerced = vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        start_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ];
                if let Some(len_arg) = args.get(2) {
                    let len_type = infer_sql_expr_type_with_ctes(
                        len_arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    if !is_integer_family(len_type) {
                        return Err(ParseError::UnexpectedToken {
                            expected: "integer length argument",
                            actual: sql_type_name(len_type),
                        });
                    }
                    coerced.push(coerce_bound_expr(
                        bound_args[2].clone(),
                        len_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ));
                }
                return Ok(build_func(func_variadic, coerced));
            }
            if !is_text_like_type(value_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "substring(text, int4[, int4]) or substring(text, text[, text])",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(value_type),
                        sql_type_name(start_type)
                    ),
                });
            }
            let text_target = coerce_bound_expr(
                bound_args[0].clone(),
                value_type,
                SqlType::new(SqlTypeKind::Text),
            );
            if start_type.kind == SqlTypeKind::Text {
                let mut coerced = vec![
                    text_target,
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        start_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ];
                if let Some(third_arg) = args.get(2) {
                    let third_type = infer_sql_expr_type_with_ctes(
                        third_arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    if third_type.kind != SqlTypeKind::Text {
                        return Err(ParseError::UnexpectedToken {
                            expected: "text escape argument",
                            actual: sql_type_name(third_type),
                        });
                    }
                    coerced.push(coerce_bound_expr(
                        bound_args[2].clone(),
                        third_type,
                        SqlType::new(SqlTypeKind::Text),
                    ));
                    return Ok(Expr::builtin_func(
                        BuiltinScalarFunction::SimilarSubstring,
                        result_type,
                        func_variadic,
                        coerced,
                    ));
                }
                return Ok(build_func(func_variadic, coerced));
            }
            if !is_integer_family(start_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "substring(text, int4[, int4]) or substring(text, text[, text])",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(value_type),
                        sql_type_name(start_type)
                    ),
                });
            }
            let mut coerced = vec![
                text_target,
                coerce_bound_expr(
                    bound_args[1].clone(),
                    start_type,
                    SqlType::new(SqlTypeKind::Int4),
                ),
            ];
            if let Some(len_arg) = args.get(2) {
                let len_type = infer_sql_expr_type_with_ctes(
                    len_arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                if !is_integer_family(len_type) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "integer length argument",
                        actual: sql_type_name(len_type),
                    });
                }
                coerced.push(coerce_bound_expr(
                    bound_args[2].clone(),
                    len_type,
                    SqlType::new(SqlTypeKind::Int4),
                ));
            }
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::SimilarSubstring => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let pattern_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !is_text_like_type(value_type) || !is_text_like_type(pattern_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "substring(text similar text escape text)",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(value_type),
                        sql_type_name(pattern_type)
                    ),
                });
            }
            let mut coerced = vec![
                coerce_bound_expr(
                    bound_args[0].clone(),
                    value_type,
                    SqlType::new(SqlTypeKind::Text),
                ),
                coerce_bound_expr(
                    bound_args[1].clone(),
                    pattern_type,
                    SqlType::new(SqlTypeKind::Text),
                ),
            ];
            if let Some(escape_arg) = args.get(2) {
                let escape_type = infer_sql_expr_type_with_ctes(
                    escape_arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                if !is_text_like_type(escape_type) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "text escape argument",
                        actual: sql_type_name(escape_type),
                    });
                }
                coerced.push(coerce_bound_expr(
                    bound_args[2].clone(),
                    escape_type,
                    SqlType::new(SqlTypeKind::Text),
                ));
            }
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::Overlay => {
            let raw_value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let raw_place_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let value_type =
                coerce_unknown_string_literal_type(&args[0], raw_value_type, raw_place_type);
            let place_type =
                coerce_unknown_string_literal_type(&args[1], raw_place_type, value_type);
            let start_type = infer_sql_expr_type_with_ctes(
                &args[2],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let same_bit_kind = is_bit_string_type(value_type) && is_bit_string_type(place_type);
            let same_bytea_kind =
                value_type.kind == SqlTypeKind::Bytea && place_type.kind == SqlTypeKind::Bytea;
            let same_text_kind = is_text_like_type(value_type) && is_text_like_type(place_type);
            if (!same_bit_kind && !same_bytea_kind && !same_text_kind)
                || !is_integer_family(start_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "overlay(text, text, int4[, int4]), overlay(bit, bit, int4[, int4]) or overlay(bytea, bytea, int4[, int4])",
                    actual: format!(
                        "{func:?}({}, {}, {})",
                        sql_type_name(value_type),
                        sql_type_name(place_type),
                        sql_type_name(start_type)
                    ),
                });
            }
            let mut coerced = if same_bytea_kind {
                vec![
                    bound_args[0].clone(),
                    bound_args[1].clone(),
                    coerce_bound_expr(
                        bound_args[2].clone(),
                        start_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ]
            } else if same_text_kind {
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        raw_value_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        raw_place_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                    coerce_bound_expr(
                        bound_args[2].clone(),
                        start_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ]
            } else {
                let common = resolve_common_scalar_type(value_type, place_type)
                    .unwrap_or(SqlType::new(SqlTypeKind::VarBit));
                vec![
                    coerce_bound_expr(bound_args[0].clone(), raw_value_type, common),
                    coerce_bound_expr(bound_args[1].clone(), raw_place_type, common),
                    coerce_bound_expr(
                        bound_args[2].clone(),
                        start_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ]
            };
            if let Some(len_arg) = args.get(3) {
                let len_type = infer_sql_expr_type_with_ctes(
                    len_arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                if !is_integer_family(len_type) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "integer length argument",
                        actual: sql_type_name(len_type),
                    });
                }
                coerced.push(coerce_bound_expr(
                    bound_args[3].clone(),
                    len_type,
                    SqlType::new(SqlTypeKind::Int4),
                ));
            }
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::GetBit => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let index_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !(is_bit_string_type(value_type) || value_type.kind == SqlTypeKind::Bytea)
                || !is_integer_family(index_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "get_bit(bit, int4) or get_bit(bytea, int4)",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(value_type),
                        sql_type_name(index_type)
                    ),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        index_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::SetBit => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let index_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let bit_type = infer_sql_expr_type_with_ctes(
                &args[2],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !(is_bit_string_type(value_type) || value_type.kind == SqlTypeKind::Bytea)
                || !is_integer_family(index_type)
                || !is_integer_family(bit_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "set_bit(bit, int4, int4) or set_bit(bytea, int4, int4)",
                    actual: format!(
                        "{func:?}({}, {}, {})",
                        sql_type_name(value_type),
                        sql_type_name(index_type),
                        sql_type_name(bit_type)
                    ),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        index_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                    coerce_bound_expr(
                        bound_args[2].clone(),
                        bit_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::BitCount => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !(is_bit_string_type(value_type) || value_type.kind == SqlTypeKind::Bytea) {
                return Err(ParseError::UnexpectedToken {
                    expected: "bit or bytea argument",
                    actual: format!("{func:?}({})", sql_type_name(value_type)),
                });
            }
            Ok(build_func(func_variadic, vec![bound_args[0].clone()]))
        }
        BuiltinScalarFunction::GetByte => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let index_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if value_type.kind != SqlTypeKind::Bytea || !is_integer_family(index_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "get_byte(bytea, int4)",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(value_type),
                        sql_type_name(index_type)
                    ),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        index_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::SetByte => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let index_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let new_type = infer_sql_expr_type_with_ctes(
                &args[2],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if value_type.kind != SqlTypeKind::Bytea
                || !is_integer_family(index_type)
                || !is_integer_family(new_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "set_byte(bytea, int4, int4)",
                    actual: format!(
                        "{func:?}({}, {}, {})",
                        sql_type_name(value_type),
                        sql_type_name(index_type),
                        sql_type_name(new_type)
                    ),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        index_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                    coerce_bound_expr(
                        bound_args[2].clone(),
                        new_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::ConvertFrom => {
            let left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        left_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        right_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::Lower
        | BuiltinScalarFunction::Upper
        | BuiltinScalarFunction::Casefold
        | BuiltinScalarFunction::Unistr => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Text),
                )],
            ))
        }
        BuiltinScalarFunction::Initcap
        | BuiltinScalarFunction::Ascii
        | BuiltinScalarFunction::Replace
        | BuiltinScalarFunction::Translate => Ok(build_func(
            func_variadic,
            args.iter()
                .enumerate()
                .map(|(idx, arg)| {
                    let ty = infer_sql_expr_type_with_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    coerce_bound_expr(bound_args[idx].clone(), ty, SqlType::new(SqlTypeKind::Text))
                })
                .collect(),
        )),
        BuiltinScalarFunction::Chr => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !is_integer_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "integer argument",
                    actual: sql_type_name(arg_type),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Int4),
                )],
            ))
        }
        BuiltinScalarFunction::SplitPart => Ok(build_func(
            func_variadic,
            vec![
                coerce_bound_expr(
                    bound_args[0].clone(),
                    infer_sql_expr_type_with_ctes(
                        &args[0],
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    ),
                    SqlType::new(SqlTypeKind::Text),
                ),
                coerce_bound_expr(
                    bound_args[1].clone(),
                    infer_sql_expr_type_with_ctes(
                        &args[1],
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    ),
                    SqlType::new(SqlTypeKind::Text),
                ),
                coerce_bound_expr(
                    bound_args[2].clone(),
                    infer_sql_expr_type_with_ctes(
                        &args[2],
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    ),
                    SqlType::new(SqlTypeKind::Int4),
                ),
            ],
        )),
        BuiltinScalarFunction::LPad | BuiltinScalarFunction::RPad => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let len_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let mut coerced = vec![
                coerce_bound_expr(
                    bound_args[0].clone(),
                    value_type,
                    SqlType::new(SqlTypeKind::Text),
                ),
                coerce_bound_expr(
                    bound_args[1].clone(),
                    len_type,
                    SqlType::new(SqlTypeKind::Int4),
                ),
            ];
            if let Some(fill_arg) = args.get(2) {
                let fill_type = infer_sql_expr_type_with_ctes(
                    fill_arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                coerced.push(coerce_bound_expr(
                    bound_args[2].clone(),
                    fill_type,
                    SqlType::new(SqlTypeKind::Text),
                ));
            }
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::BTrim
        | BuiltinScalarFunction::LTrim
        | BuiltinScalarFunction::RTrim => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let mut coerced = vec![bound_args[0].clone()];
            if let Some(chars_arg) = args.get(1) {
                let chars_type = infer_sql_expr_type_with_ctes(
                    chars_arg,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                let target = if value_type.kind == SqlTypeKind::Bytea {
                    SqlType::new(SqlTypeKind::Bytea)
                } else {
                    SqlType::new(SqlTypeKind::Text)
                };
                coerced[0] = coerce_bound_expr(bound_args[0].clone(), value_type, target);
                coerced.push(coerce_bound_expr(bound_args[1].clone(), chars_type, target));
            } else if value_type.kind != SqlTypeKind::Bytea {
                coerced[0] = coerce_bound_expr(
                    bound_args[0].clone(),
                    value_type,
                    SqlType::new(SqlTypeKind::Text),
                );
            }
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::RegexpMatch | BuiltinScalarFunction::RegexpLike => Ok(build_func(
            func_variadic,
            args.iter()
                .enumerate()
                .map(|(idx, arg)| {
                    let ty = infer_sql_expr_type_with_ctes(
                        arg,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    let target = SqlType::new(SqlTypeKind::Text);
                    coerce_bound_expr(bound_args[idx].clone(), ty, target)
                })
                .collect(),
        )),
        BuiltinScalarFunction::RegexpCount => Ok(build_func(
            func_variadic,
            bind_regex_count_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        )),
        BuiltinScalarFunction::RegexpInstr => Ok(build_func(
            func_variadic,
            bind_regex_instr_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        )),
        BuiltinScalarFunction::RegexpSubstr => Ok(build_func(
            func_variadic,
            bind_regex_substr_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        )),
        BuiltinScalarFunction::RegexpReplace => Ok(build_func(
            func_variadic,
            bind_regex_replace_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        )),
        BuiltinScalarFunction::RegexpSplitToArray => Ok(build_func(
            func_variadic,
            bind_regex_split_to_array_args(
                &bound_args,
                args,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
        )),
        BuiltinScalarFunction::Md5 => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !matches!(arg_type.kind, SqlTypeKind::Text | SqlTypeKind::Bytea) || arg_type.is_array
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "text or bytea argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(func_variadic, vec![bound_args[0].clone()]))
        }
        BuiltinScalarFunction::Reverse
        | BuiltinScalarFunction::Sha224
        | BuiltinScalarFunction::Sha256
        | BuiltinScalarFunction::Sha384
        | BuiltinScalarFunction::Sha512
        | BuiltinScalarFunction::Crc32
        | BuiltinScalarFunction::Crc32c => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !matches!(arg_type.kind, SqlTypeKind::Text | SqlTypeKind::Bytea) || arg_type.is_array
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "text or bytea argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(func_variadic, vec![bound_args[0].clone()]))
        }
        BuiltinScalarFunction::QuoteLiteral => Ok(build_func(func_variadic, bound_args)),
        BuiltinScalarFunction::Encode => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let format_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let target_value_type = if matches!(
                &args[0],
                SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
            ) {
                SqlType::new(SqlTypeKind::Bytea)
            } else {
                value_type
            };
            if target_value_type.kind != SqlTypeKind::Bytea {
                return Err(ParseError::UnexpectedToken {
                    expected: "bytea argument",
                    actual: sql_type_name(value_type),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(bound_args[0].clone(), value_type, target_value_type),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        format_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::Decode => Ok(build_func(
            func_variadic,
            vec![
                coerce_bound_expr(
                    bound_args[0].clone(),
                    infer_sql_expr_type_with_ctes(
                        &args[0],
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    ),
                    SqlType::new(SqlTypeKind::Text),
                ),
                coerce_bound_expr(
                    bound_args[1].clone(),
                    infer_sql_expr_type_with_ctes(
                        &args[1],
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    ),
                    SqlType::new(SqlTypeKind::Text),
                ),
            ],
        )),
        BuiltinScalarFunction::ToChar => {
            let value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let format_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !is_numeric_family(value_type)
                && !matches!(
                    value_type.kind,
                    SqlTypeKind::Date
                        | SqlTypeKind::Timestamp
                        | SqlTypeKind::TimestampTz
                        | SqlTypeKind::Interval
                )
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric or datetime argument",
                    actual: format!("{func:?}({})", sql_type_name(value_type)),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        format_type,
                        SqlType::new(SqlTypeKind::Text),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::ToDate => Ok(build_func(
            func_variadic,
            arg_types
                .into_iter()
                .zip(bound_args)
                .map(|(ty, arg)| coerce_bound_expr(arg, ty, SqlType::new(SqlTypeKind::Text)))
                .collect(),
        )),
        BuiltinScalarFunction::ToTimestamp if bound_args.len() == 2 => {
            Ok(Expr::resolved_builtin_func(
                func,
                func_oid,
                Some(SqlType::new(SqlTypeKind::TimestampTz)),
                false,
                arg_types
                    .into_iter()
                    .zip(bound_args)
                    .map(|(ty, arg)| coerce_bound_expr(arg, ty, SqlType::new(SqlTypeKind::Text)))
                    .collect(),
            ))
        }
        BuiltinScalarFunction::ToTimestamp => Ok(Expr::resolved_builtin_func(
            func,
            func_oid,
            Some(SqlType::new(SqlTypeKind::TimestampTz)),
            false,
            vec![coerce_bound_expr(
                bound_args[0].clone(),
                arg_types[0],
                SqlType::new(SqlTypeKind::Float8),
            )],
        )),
        BuiltinScalarFunction::NumericInc
        | BuiltinScalarFunction::Factorial
        | BuiltinScalarFunction::PgLsn => {
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let arg_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_arg_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Numeric),
                )],
            ))
        }
        BuiltinScalarFunction::Log10 | BuiltinScalarFunction::Log if args.len() == 1 => {
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let arg_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_arg_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            let target = if matches!(
                arg_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) {
                SqlType::new(SqlTypeKind::Float8)
            } else {
                SqlType::new(SqlTypeKind::Numeric)
            };
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(bound_args[0].clone(), arg_type, target)],
            ))
        }
        BuiltinScalarFunction::Log => {
            let raw_left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let raw_right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let left_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_left_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            let right_type =
                coerce_unknown_string_literal_type(&args[1], raw_right_type, left_type);
            if !is_numeric_family(left_type) || !is_numeric_family(right_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric arguments",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(left_type),
                        sql_type_name(right_type)
                    ),
                });
            }
            let target = SqlType::new(SqlTypeKind::Numeric);
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(bound_args[0].clone(), left_type, target),
                    coerce_bound_expr(bound_args[1].clone(), right_type, target),
                ],
            ))
        }
        BuiltinScalarFunction::Gcd | BuiltinScalarFunction::Lcm => {
            let left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !is_numeric_family(left_type) || !is_numeric_family(right_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric or integer arguments",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(left_type),
                        sql_type_name(right_type)
                    ),
                });
            }
            let common = resolve_numeric_binary_type("+", left_type, right_type)?;
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(bound_args[0].clone(), left_type, common),
                    coerce_bound_expr(bound_args[1].clone(), right_type, common),
                ],
            ))
        }
        BuiltinScalarFunction::Div | BuiltinScalarFunction::Mod => {
            let raw_left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let raw_right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let left_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_left_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            let right_type = coerce_unknown_string_literal_type(
                &args[1],
                raw_right_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(left_type) || !is_numeric_family(right_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric arguments",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(left_type),
                        sql_type_name(right_type)
                    ),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        left_type,
                        SqlType::new(SqlTypeKind::Numeric),
                    ),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        right_type,
                        SqlType::new(SqlTypeKind::Numeric),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::Scale
        | BuiltinScalarFunction::MinScale
        | BuiltinScalarFunction::TrimScale => {
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let arg_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_arg_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Numeric),
                )],
            ))
        }
        BuiltinScalarFunction::WidthBucket => {
            if args.len() == 2 {
                let operand_type = infer_sql_expr_type_with_ctes(
                    &args[0],
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                let thresholds_type = infer_sql_expr_type_with_ctes(
                    &args[1],
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                if thresholds_type.is_array {
                    let element_type = thresholds_type.element_type();
                    let operand_type =
                        coerce_unknown_string_literal_type(&args[0], operand_type, element_type);
                    if !width_bucket_threshold_types_compatible(operand_type, element_type) {
                        return Err(function_does_not_exist_error(
                            "width_bucket",
                            &[operand_type, thresholds_type],
                            catalog,
                        ));
                    }
                }
                return Ok(build_func(func_variadic, bound_args));
            }
            let raw_operand_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let raw_low_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let raw_high_type = infer_sql_expr_type_with_ctes(
                &args[2],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let count_type = infer_sql_expr_type_with_ctes(
                &args[3],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let initial_operand_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_operand_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            let initial_low_type =
                coerce_unknown_string_literal_type(&args[1], raw_low_type, initial_operand_type);
            let initial_high_type =
                coerce_unknown_string_literal_type(&args[2], raw_high_type, initial_operand_type);
            if !is_numeric_family(initial_operand_type)
                || !is_numeric_family(initial_low_type)
                || !is_numeric_family(initial_high_type)
                || !is_integer_family(count_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "width_bucket(numeric, numeric, numeric, int4)",
                    actual: format!(
                        "{func:?}({}, {}, {}, {})",
                        sql_type_name(initial_operand_type),
                        sql_type_name(initial_low_type),
                        sql_type_name(initial_high_type),
                        sql_type_name(count_type)
                    ),
                });
            }
            let target = if matches!(
                initial_operand_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) || matches!(
                initial_low_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) || matches!(
                initial_high_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) {
                SqlType::new(SqlTypeKind::Float8)
            } else {
                SqlType::new(SqlTypeKind::Numeric)
            };
            let operand_type =
                coerce_unknown_string_literal_type(&args[0], raw_operand_type, target);
            let low_type = coerce_unknown_string_literal_type(&args[1], raw_low_type, target);
            let high_type = coerce_unknown_string_literal_type(&args[2], raw_high_type, target);
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(bound_args[0].clone(), operand_type, target),
                    coerce_bound_expr(bound_args[1].clone(), low_type, target),
                    coerce_bound_expr(bound_args[2].clone(), high_type, target),
                    coerce_bound_expr(
                        bound_args[3].clone(),
                        count_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::Trunc | BuiltinScalarFunction::Round if args.len() == 2 => {
            let raw_value_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let value_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_value_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            let scale_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !matches!(value_type.element_type().kind, SqlTypeKind::Numeric)
                || !is_integer_family(scale_type)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric, integer arguments",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(value_type),
                        sql_type_name(scale_type)
                    ),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        value_type,
                        SqlType::new(SqlTypeKind::Numeric),
                    ),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        scale_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::Trunc | BuiltinScalarFunction::Round => {
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let arg_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_arg_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            let target = if matches!(
                arg_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) {
                SqlType::new(SqlTypeKind::Float8)
            } else {
                SqlType::new(SqlTypeKind::Numeric)
            };
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(bound_args[0].clone(), arg_type, target)],
            ))
        }
        BuiltinScalarFunction::Ceil
        | BuiltinScalarFunction::Ceiling
        | BuiltinScalarFunction::Floor
        | BuiltinScalarFunction::Sign
            if args.len() == 1 =>
        {
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let arg_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_arg_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            let target = if matches!(
                arg_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) {
                SqlType::new(SqlTypeKind::Float8)
            } else {
                SqlType::new(SqlTypeKind::Numeric)
            };
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(bound_args[0].clone(), arg_type, target)],
            ))
        }
        BuiltinScalarFunction::Ceil
        | BuiltinScalarFunction::Ceiling
        | BuiltinScalarFunction::Floor
        | BuiltinScalarFunction::Sign
        | BuiltinScalarFunction::Cbrt
        | BuiltinScalarFunction::Sin
        | BuiltinScalarFunction::Cos
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
        | BuiltinScalarFunction::Erf
        | BuiltinScalarFunction::Erfc
        | BuiltinScalarFunction::Gamma
        | BuiltinScalarFunction::Lgamma => {
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let arg_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_arg_type,
                SqlType::new(SqlTypeKind::Float8),
            );
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    SqlType::new(SqlTypeKind::Float8),
                )],
            ))
        }
        BuiltinScalarFunction::Sqrt | BuiltinScalarFunction::Exp | BuiltinScalarFunction::Ln => {
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let arg_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_arg_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            let target = if matches!(
                arg_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) {
                SqlType::new(SqlTypeKind::Float8)
            } else {
                SqlType::new(SqlTypeKind::Numeric)
            };
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(bound_args[0].clone(), arg_type, target)],
            ))
        }
        BuiltinScalarFunction::BitcastIntegerToFloat4 => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if arg_type != SqlType::new(SqlTypeKind::Int4) {
                return Err(ParseError::UnexpectedToken {
                    expected: "integer argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(func_variadic, vec![bound_args[0].clone()]))
        }
        BuiltinScalarFunction::BitcastBigintToFloat8 => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if arg_type != SqlType::new(SqlTypeKind::Int8) {
                return Err(ParseError::UnexpectedToken {
                    expected: "bigint argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(func_variadic, vec![bound_args[0].clone()]))
        }
        BuiltinScalarFunction::Atan2d => {
            let raw_left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let raw_right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let left_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_left_type,
                SqlType::new(SqlTypeKind::Float8),
            );
            let right_type = coerce_unknown_string_literal_type(
                &args[1],
                raw_right_type,
                SqlType::new(SqlTypeKind::Float8),
            );
            if !is_numeric_family(left_type) || !is_numeric_family(right_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric arguments",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(left_type),
                        sql_type_name(right_type)
                    ),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(
                        bound_args[0].clone(),
                        left_type,
                        SqlType::new(SqlTypeKind::Float8),
                    ),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        right_type,
                        SqlType::new(SqlTypeKind::Float8),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::Power => {
            let raw_left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let raw_right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let left_type = coerce_unknown_string_literal_type(
                &args[0],
                raw_left_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            let right_type = coerce_unknown_string_literal_type(
                &args[1],
                raw_right_type,
                SqlType::new(SqlTypeKind::Numeric),
            );
            if !is_numeric_family(left_type) || !is_numeric_family(right_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric arguments",
                    actual: format!(
                        "{func:?}({}, {})",
                        sql_type_name(left_type),
                        sql_type_name(right_type)
                    ),
                });
            }
            let target = if matches!(
                left_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) || matches!(
                right_type.element_type().kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            ) {
                SqlType::new(SqlTypeKind::Float8)
            } else {
                SqlType::new(SqlTypeKind::Numeric)
            };
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(bound_args[0].clone(), left_type, target),
                    coerce_bound_expr(bound_args[1].clone(), right_type, target),
                ],
            ))
        }
        BuiltinScalarFunction::Float4Send | BuiltinScalarFunction::Float8Send => {
            let arg_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let target_type = if matches!(func, BuiltinScalarFunction::Float4Send) {
                SqlType::new(SqlTypeKind::Float4)
            } else {
                SqlType::new(SqlTypeKind::Float8)
            };
            if !is_numeric_family(arg_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "numeric argument",
                    actual: format!("{func:?}({})", sql_type_name(arg_type)),
                });
            }
            Ok(build_func(
                func_variadic,
                vec![coerce_bound_expr(
                    bound_args[0].clone(),
                    arg_type,
                    target_type,
                )],
            ))
        }
        BuiltinScalarFunction::PgInputIsValid
        | BuiltinScalarFunction::PgInputErrorMessage
        | BuiltinScalarFunction::PgInputErrorDetail
        | BuiltinScalarFunction::PgInputErrorHint
        | BuiltinScalarFunction::PgInputErrorSqlState => {
            let text_type = SqlType::new(SqlTypeKind::Text);
            let left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            Ok(build_func(
                func_variadic,
                vec![
                    coerce_bound_expr(bound_args[0].clone(), left_type, text_type),
                    coerce_bound_expr(bound_args[1].clone(), right_type, text_type),
                ],
            ))
        }
        BuiltinScalarFunction::PgTypeof => {
            if expr_contains_set_returning(&bound_args[0]) {
                return Ok(build_func(func_variadic, bound_args));
            }
            let arg_type = if matches!(
                &args[0],
                SqlExpr::Const(Value::Null)
                    | SqlExpr::Const(Value::Text(_))
                    | SqlExpr::Const(Value::TextRef(_, _))
            ) {
                SqlType::new(SqlTypeKind::Text).with_identity(UNKNOWN_TYPE_OID, 0)
            } else {
                super::super::infer::infer_sql_expr_function_arg_type_with_ctes(
                    &args[0],
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            };
            if arg_type.type_oid == UNKNOWN_TYPE_OID {
                return Ok(Expr::Cast(
                    Box::new(Expr::Const(Value::Int64(UNKNOWN_TYPE_OID as i64))),
                    SqlType::new(SqlTypeKind::RegType),
                ));
            }
            Ok(Expr::Cast(
                Box::new(Expr::Const(Value::Text(
                    pg_typeof_sql_type_name(arg_type, catalog).into(),
                ))),
                SqlType::new(SqlTypeKind::RegType),
            ))
        }
        BuiltinScalarFunction::JsonbDeletePath
        | BuiltinScalarFunction::JsonbSet
        | BuiltinScalarFunction::JsonbSetLax
        | BuiltinScalarFunction::JsonbInsert => {
            let path_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let target_path_type = if matches!(
                &args[1],
                SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
            ) {
                SqlType::array_of(SqlType::new(SqlTypeKind::Text))
            } else {
                path_type
            };
            let mut rewritten = rewritten_bound_args;
            rewritten[1] = coerce_bound_expr(rewritten[1].clone(), path_type, target_path_type);
            Ok(build_func(func_variadic, rewritten))
        }
        BuiltinScalarFunction::JsonbConcat => {
            let jsonb_type = SqlType::new(SqlTypeKind::Jsonb);
            let coerced = bound_args
                .into_iter()
                .zip(arg_types)
                .map(|(arg, actual_type)| coerce_bound_expr(arg, actual_type, jsonb_type))
                .collect();
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::ArrayNdims | BuiltinScalarFunction::ArrayDims => {
            Ok(build_func(func_variadic, vec![bound_args[0].clone()]))
        }
        BuiltinScalarFunction::ArrayLower => {
            let dim_type = infer_sql_expr_type_with_ctes(
                &args[1],
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            Ok(build_func(
                func_variadic,
                vec![
                    bound_args[0].clone(),
                    coerce_bound_expr(
                        bound_args[1].clone(),
                        dim_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                ],
            ))
        }
        BuiltinScalarFunction::JsonObject | BuiltinScalarFunction::JsonbObject => {
            let coerced = bound_args
                .into_iter()
                .zip(arg_types)
                .zip(declared_arg_types.iter().copied())
                .map(|((arg, actual_type), declared_type)| {
                    coerce_bound_expr(arg, actual_type, declared_type)
                })
                .collect();
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::JsonPopulateRecord
        | BuiltinScalarFunction::JsonPopulateRecordValid
        | BuiltinScalarFunction::JsonToRecord
        | BuiltinScalarFunction::JsonbPopulateRecord
        | BuiltinScalarFunction::JsonbPopulateRecordValid
        | BuiltinScalarFunction::JsonbToRecord => {
            let coerced = bound_args
                .into_iter()
                .zip(arg_types)
                .zip(declared_arg_types.iter().copied())
                .map(|((arg, actual_type), declared_type)| {
                    coerce_bound_expr(arg, actual_type, declared_type)
                })
                .collect();
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::JsonbArrayLength => {
            let target_type = declared_arg_types
                .first()
                .copied()
                .unwrap_or(SqlType::new(SqlTypeKind::Jsonb));
            let raw_arg_type = arg_types[0];
            let resolved_arg_type =
                coerce_unknown_string_literal_type(&args[0], raw_arg_type, target_type);
            Ok(build_func(
                func_variadic,
                vec![
                    if resolved_arg_type == target_type && raw_arg_type != target_type {
                        coerce_bound_expr(bound_args[0].clone(), raw_arg_type, target_type)
                    } else {
                        bound_args[0].clone()
                    },
                ],
            ))
        }
        BuiltinScalarFunction::PgPartitionRoot => {
            let coerced = bound_args
                .into_iter()
                .zip(arg_types)
                .zip(declared_arg_types.iter().copied())
                .map(|((arg, actual_type), declared_type)| {
                    coerce_bound_expr(arg, actual_type, declared_type)
                })
                .collect();
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::RangeConstructor
        | BuiltinScalarFunction::RangeIsEmpty
        | BuiltinScalarFunction::RangeLower
        | BuiltinScalarFunction::RangeUpper
        | BuiltinScalarFunction::RangeLowerInc
        | BuiltinScalarFunction::RangeUpperInc
        | BuiltinScalarFunction::RangeLowerInf
        | BuiltinScalarFunction::RangeUpperInf
        | BuiltinScalarFunction::RangeContains
        | BuiltinScalarFunction::RangeContainedBy
        | BuiltinScalarFunction::RangeOverlap
        | BuiltinScalarFunction::RangeStrictLeft
        | BuiltinScalarFunction::RangeStrictRight
        | BuiltinScalarFunction::RangeOverLeft
        | BuiltinScalarFunction::RangeOverRight
        | BuiltinScalarFunction::RangeAdjacent
        | BuiltinScalarFunction::RangeUnion
        | BuiltinScalarFunction::RangeIntersect
        | BuiltinScalarFunction::RangeDifference
        | BuiltinScalarFunction::RangeMerge => {
            let fallback_declared = if !declared_arg_types.is_empty() {
                declared_arg_types.to_vec()
            } else if matches!(func, BuiltinScalarFunction::RangeConstructor) && args.is_empty() {
                Vec::new()
            } else if matches!(func, BuiltinScalarFunction::RangeConstructor) {
                let range_type = result_type
                    .and_then(range_type_ref_for_sql_type)
                    .ok_or_else(|| ParseError::UnexpectedToken {
                        expected: "range constructor with a concrete range return type",
                        actual: format!("{func:?}"),
                    })?;
                let mut types = vec![range_type.subtype, range_type.subtype];
                if args.len() == 3 {
                    types.push(SqlType::new(SqlTypeKind::Text));
                }
                types
            } else {
                arg_types.clone()
            };
            let coerced = bound_args
                .into_iter()
                .zip(arg_types)
                .zip(fallback_declared.into_iter())
                .map(|((arg, actual_type), declared_type)| {
                    coerce_bound_expr(arg, actual_type, declared_type)
                })
                .collect();
            Ok(build_func(func_variadic, coerced))
        }
        BuiltinScalarFunction::MacAddrEq
        | BuiltinScalarFunction::MacAddrNe
        | BuiltinScalarFunction::MacAddrLt
        | BuiltinScalarFunction::MacAddrLe
        | BuiltinScalarFunction::MacAddrGt
        | BuiltinScalarFunction::MacAddrGe
        | BuiltinScalarFunction::MacAddrCmp
        | BuiltinScalarFunction::MacAddrNot
        | BuiltinScalarFunction::MacAddrAnd
        | BuiltinScalarFunction::MacAddrOr
        | BuiltinScalarFunction::MacAddrTrunc
        | BuiltinScalarFunction::MacAddrToMacAddr8
        | BuiltinScalarFunction::MacAddr8Eq
        | BuiltinScalarFunction::MacAddr8Ne
        | BuiltinScalarFunction::MacAddr8Lt
        | BuiltinScalarFunction::MacAddr8Le
        | BuiltinScalarFunction::MacAddr8Gt
        | BuiltinScalarFunction::MacAddr8Ge
        | BuiltinScalarFunction::MacAddr8Cmp
        | BuiltinScalarFunction::MacAddr8Not
        | BuiltinScalarFunction::MacAddr8And
        | BuiltinScalarFunction::MacAddr8Or
        | BuiltinScalarFunction::MacAddr8Trunc
        | BuiltinScalarFunction::MacAddr8ToMacAddr
        | BuiltinScalarFunction::MacAddr8Set7Bit
        | BuiltinScalarFunction::HashMacAddr
        | BuiltinScalarFunction::HashMacAddrExtended
        | BuiltinScalarFunction::HashMacAddr8
        | BuiltinScalarFunction::HashMacAddr8Extended
        | BuiltinScalarFunction::HashValue(_)
        | BuiltinScalarFunction::HashValueExtended(_)
        | BuiltinScalarFunction::TxidSnapshotXmin
        | BuiltinScalarFunction::TxidSnapshotXmax
        | BuiltinScalarFunction::TxidVisibleInSnapshot
        | BuiltinScalarFunction::TxidStatus => {
            let coerced = bound_args
                .into_iter()
                .zip(arg_types)
                .zip(declared_arg_types.iter().copied())
                .map(|((arg, actual_type), declared_type)| {
                    coerce_bound_expr(arg, actual_type, declared_type)
                })
                .collect();
            Ok(build_func(func_variadic, coerced))
        }
        _ => Ok(build_func(func_variadic, rewritten_bound_args)),
    }
}

fn preserve_satisfies_hash_partition_arg_types(
    bound_args: Vec<Expr>,
    arg_types: &[SqlType],
) -> Vec<Expr> {
    bound_args
        .into_iter()
        .zip(arg_types.iter().copied())
        .map(|(arg, sql_type)| {
            if expr_sql_type_hint(&arg).is_none() {
                Expr::Cast(Box::new(arg), sql_type)
            } else {
                arg
            }
        })
        .collect()
}

fn rewrite_variadic_bound_args(
    bound_args: Vec<Expr>,
    arg_types: &[SqlType],
    declared_arg_types: &[SqlType],
    func_variadic: bool,
    nvargs: usize,
    vatype_oid: u32,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Expr>, ParseError> {
    if !func_variadic {
        return Ok(bound_args);
    }
    if vatype_oid == 0 {
        return Ok(bound_args);
    }
    if vatype_oid == ANYOID {
        return Ok(bound_args);
    }

    if nvargs > 0 {
        let fixed_prefix_len = bound_args.len().saturating_sub(nvargs);
        let element_type = variadic_rewrite_element_type(
            vatype_oid,
            declared_arg_types.get(fixed_prefix_len).copied(),
            arg_types.get(fixed_prefix_len).copied(),
            catalog,
        )?;
        let array_type = SqlType::array_of(element_type);
        let mut rewritten = bound_args[..fixed_prefix_len].to_vec();
        let elements = bound_args[fixed_prefix_len..]
            .iter()
            .zip(arg_types[fixed_prefix_len..].iter())
            .map(|(expr, sql_type)| coerce_bound_expr(expr.clone(), *sql_type, element_type))
            .collect();
        rewritten.push(Expr::ArrayLiteral {
            elements,
            array_type,
        });
        return Ok(rewritten);
    }

    let mut rewritten = bound_args;
    if let (Some(last), Some(last_type)) = (rewritten.last_mut(), arg_types.last()) {
        let target_array_type = explicit_variadic_rewrite_array_type(
            vatype_oid,
            declared_arg_types.last().copied(),
            *last_type,
            catalog,
        )?;
        *last = coerce_bound_expr(last.clone(), *last_type, target_array_type);
    }
    Ok(rewritten)
}

fn variadic_rewrite_element_type(
    vatype_oid: u32,
    declared_type: Option<SqlType>,
    actual_type: Option<SqlType>,
    catalog: &dyn CatalogLookup,
) -> Result<SqlType, ParseError> {
    if matches!(vatype_oid, ANYELEMENTOID | ANYCOMPATIBLEOID | ANYENUMOID) {
        return declared_type
            .or(actual_type)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "known polymorphic variadic element type",
                actual: vatype_oid.to_string(),
            });
    }
    catalog
        .type_by_oid(vatype_oid)
        .map(|row| row.sql_type)
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "known variadic element type",
            actual: vatype_oid.to_string(),
        })
}

fn explicit_variadic_rewrite_array_type(
    vatype_oid: u32,
    declared_type: Option<SqlType>,
    actual_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<SqlType, ParseError> {
    if matches!(vatype_oid, ANYELEMENTOID | ANYCOMPATIBLEOID | ANYENUMOID) {
        return Ok(declared_type.unwrap_or(actual_type));
    }
    let element_type = variadic_rewrite_element_type(vatype_oid, None, None, catalog)?;
    Ok(SqlType::array_of(element_type))
}

fn bind_regex_count_args(
    bound_args: &[Expr],
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Vec<Expr> {
    let mut out = Vec::with_capacity(args.len());
    for (idx, arg) in args.iter().enumerate() {
        let ty =
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes);
        out.push(coerce_bound_expr(
            bound_args[idx].clone(),
            ty,
            if idx == 2 {
                SqlType::new(SqlTypeKind::Int4)
            } else {
                SqlType::new(SqlTypeKind::Text)
            },
        ));
    }
    out
}

fn bind_regex_instr_args(
    bound_args: &[Expr],
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Vec<Expr> {
    let mut out = Vec::with_capacity(args.len());
    for (idx, arg) in args.iter().enumerate() {
        let ty =
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes);
        out.push(coerce_bound_expr(
            bound_args[idx].clone(),
            ty,
            match idx {
                2..=4 | 6 => SqlType::new(SqlTypeKind::Int4),
                _ => SqlType::new(SqlTypeKind::Text),
            },
        ));
    }
    out
}

fn bind_regex_substr_args(
    bound_args: &[Expr],
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Vec<Expr> {
    let mut out = Vec::with_capacity(args.len());
    for (idx, arg) in args.iter().enumerate() {
        let ty =
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes);
        out.push(coerce_bound_expr(
            bound_args[idx].clone(),
            ty,
            match idx {
                2 | 3 | 5 => SqlType::new(SqlTypeKind::Int4),
                _ => SqlType::new(SqlTypeKind::Text),
            },
        ));
    }
    out
}

fn bind_regex_replace_args(
    bound_args: &[Expr],
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Vec<Expr> {
    let mut out = Vec::with_capacity(args.len());
    let fourth_is_text = if args.len() == 4 {
        let arg_type = infer_sql_expr_type_with_ctes(
            &args[3],
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
        !is_integer_family(arg_type)
    } else {
        false
    };
    for (idx, arg) in args.iter().enumerate() {
        let ty =
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes);
        out.push(coerce_bound_expr(
            bound_args[idx].clone(),
            ty,
            match idx {
                0..=2 => SqlType::new(SqlTypeKind::Text),
                3 if fourth_is_text => SqlType::new(SqlTypeKind::Text),
                3 | 4 => SqlType::new(SqlTypeKind::Int4),
                _ => SqlType::new(SqlTypeKind::Text),
            },
        ));
    }
    out
}

fn bind_regex_split_to_array_args(
    bound_args: &[Expr],
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Vec<Expr> {
    let mut out = Vec::with_capacity(args.len());
    for (idx, arg) in args.iter().enumerate() {
        let ty =
            infer_sql_expr_type_with_ctes(arg, scope, catalog, outer_scopes, grouped_outer, ctes);
        out.push(coerce_bound_expr(
            bound_args[idx].clone(),
            ty,
            SqlType::new(SqlTypeKind::Text),
        ));
    }
    out
}

fn width_bucket_threshold_types_compatible(operand: SqlType, threshold: SqlType) -> bool {
    let operand = operand.element_type();
    let threshold = threshold.element_type();
    operand == threshold
        || (is_numeric_family(operand) && is_numeric_family(threshold))
        || (is_text_like_type(operand) && is_text_like_type(threshold))
        || (!is_text_like_type(operand) && !is_text_like_type(threshold))
}
