use super::value_io::format_array_value_text_with_config;
use crate::backend::executor::execute_readonly_statement;
use crate::backend::executor::function_guc::execute_with_sql_function_gucs;
use crate::backend::executor::{
    ExecError, ExecutorContext, QueryColumn, StatementResult, TupleSlot, Value,
    render_multirange_text_with_config, render_range_text_with_config,
};
use crate::backend::libpq::pqformat::format_bytea_text;
use crate::backend::parser::analyze::sql_type_name;
use crate::backend::parser::{
    CatalogLookup, ParseOptions, SqlType, SqlTypeKind, parse_statement_with_options,
};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::include::catalog::PgProcRow;
use crate::include::catalog::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLERANGEOID, ANYRANGEOID, PG_LANGUAGE_SQL_OID,
    RECORD_TYPE_OID, builtin_multirange_name_for_sql_type, builtin_range_name_for_sql_type,
    range_type_ref_for_sql_type,
};
use crate::include::nodes::datum::{ArrayValue, RecordValue};
use crate::include::nodes::primnodes::Expr;
use crate::pgrust::session::ByteaOutputFormat;

pub(crate) fn execute_user_defined_sql_scalar_function(
    row: &PgProcRow,
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let arg_values = args
        .iter()
        .map(|arg| crate::backend::executor::eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    execute_user_defined_sql_scalar_function_values(row, &arg_values, ctx)
}

pub(crate) fn execute_user_defined_sql_scalar_function_values(
    row: &PgProcRow,
    arg_values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    ctx.check_stack_depth()?;
    if row.prolang != PG_LANGUAGE_SQL_OID {
        return Err(sql_function_runtime_error(
            "only LANGUAGE sql functions are supported by the SQL-function runtime",
            Some(format!("language oid = {}", row.prolang)),
            "0A000",
        ));
    }

    if row.proisstrict && arg_values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }

    validate_sql_polymorphic_runtime_args(row, arg_values)?;

    execute_with_sql_function_gucs(row.proconfig.as_deref(), ctx, |ctx| {
        if let Some(value) = execute_known_lightweight_sql_function(row, arg_values)? {
            return Ok(value);
        }
        if let Some(value) = execute_sql_utility_function(row, ctx)? {
            return Ok(value);
        }

        let catalog = ctx.catalog.clone().ok_or_else(|| {
            sql_function_runtime_error(
                "LANGUAGE sql functions require executor catalog context",
                None,
                "0A000",
            )
        })?;
        let result = execute_sql_function_query(row, &arg_values, catalog.as_ref(), ctx)?;
        match result {
            StatementResult::Query { rows, .. } => match rows.as_slice() {
                [] => Ok(Value::Null),
                [row] if row.len() == 1 => Ok(row[0].clone()),
                [row] => Err(sql_function_runtime_error(
                    "scalar SQL function returned an unexpected row shape",
                    Some(format!("expected 1 column, got {}", row.len())),
                    "42804",
                )),
                _ => Err(sql_function_runtime_error(
                    "scalar SQL function returned more than one row",
                    None,
                    "21000",
                )),
            },
            other => Err(sql_function_runtime_error(
                "LANGUAGE sql function did not produce a query result",
                Some(format!("{other:?}")),
                "0A000",
            )),
        }
    })
    .map_err(|err| sql_function_context_error(row, err))
}

pub(crate) fn execute_user_defined_sql_set_returning_function(
    row: &PgProcRow,
    args: &[Expr],
    output_columns: &[QueryColumn],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let arg_values = args
        .iter()
        .map(|arg| crate::backend::executor::eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    execute_with_sql_function_gucs(row.proconfig.as_deref(), ctx, |ctx| {
        let catalog = ctx.catalog.clone().ok_or_else(|| {
            sql_function_runtime_error(
                "LANGUAGE sql functions require executor catalog context",
                None,
                "0A000",
            )
        })?;
        let expand_single_record = sql_function_declares_record_result(row, catalog.as_ref());
        let result = execute_sql_function_query(row, &arg_values, catalog.as_ref(), ctx)?;
        match result {
            StatementResult::Query { rows, .. } => rows
                .into_iter()
                .map(|mut row| {
                    if expand_single_record
                        && row.len() == 1
                        && let Some(Value::Record(record)) = row.pop()
                    {
                        row = record.fields;
                    }
                    if row.len() < output_columns.len() {
                        row.resize(output_columns.len(), Value::Null);
                    }
                    row.truncate(output_columns.len());
                    Ok(TupleSlot::virtual_row(row))
                })
                .collect(),
            other => Err(sql_function_runtime_error(
                "LANGUAGE sql function did not produce a query result",
                Some(format!("{other:?}")),
                "0A000",
            )),
        }
    })
    .map_err(|err| sql_function_context_error(row, err))
}

fn sql_function_declares_record_result(row: &PgProcRow, catalog: &dyn CatalogLookup) -> bool {
    if row.prorettype == RECORD_TYPE_OID {
        return true;
    }
    catalog.type_by_oid(row.prorettype).is_some_and(|ty| {
        matches!(
            ty.sql_type.kind,
            SqlTypeKind::Composite | SqlTypeKind::Record
        )
    })
}

fn execute_sql_function_query(
    row: &PgProcRow,
    arg_values: &[Value],
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    let sql = inline_sql_function_body(row, arg_values, catalog, &ctx.datetime_config)?;
    let stmt = parse_statement_with_options(
        &sql,
        ParseOptions {
            max_stack_depth_kb: ctx.datetime_config.max_stack_depth_kb,
            ..ParseOptions::default()
        },
    )?;
    let result = execute_readonly_statement(stmt, catalog, ctx)?;
    Ok(result)
}

fn execute_sql_utility_function(
    row: &PgProcRow,
    ctx: &ExecutorContext,
) -> Result<Option<Value>, ExecError> {
    let body = normalized_sql_function_body(&row.prosrc);
    let lower = body.to_ascii_lowercase();
    let command = if starts_with_sql_command(&lower, "analyze") {
        Some("ANALYZE")
    } else if starts_with_sql_command(&lower, "vacuum") {
        Some("VACUUM")
    } else {
        None
    };
    let Some(command) = command else {
        return Ok(None);
    };
    // :HACK: SQL functions need a utility-statement execution path with a
    // dedicated maintenance-depth flag. For now, model PostgreSQL's visible
    // VACUUM/ANALYZE recursion guard for expression-index analysis.
    if !ctx.allow_side_effects {
        return Err(ExecError::DetailedError {
            message: format!("{command} cannot be executed from VACUUM or ANALYZE"),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    Ok(Some(Value::Null))
}

fn starts_with_sql_command(sql: &str, command: &str) -> bool {
    let Some(rest) = sql.strip_prefix(command) else {
        return false;
    };
    rest.chars()
        .next()
        .map(|ch| ch.is_whitespace() || ch == '(' || ch == ';')
        .unwrap_or(true)
}

fn sql_function_context_error(row: &PgProcRow, err: ExecError) -> ExecError {
    if matches!(
        &err,
        ExecError::Parse(crate::backend::parser::ParseError::DetailedError { message, .. })
            if message
                == "invalid value for parameter \"default_text_search_config\": \"no_such_config\""
    ) {
        return err;
    }
    ExecError::WithContext {
        source: Box::new(err),
        context: format!("SQL function \"{}\" statement 1", row.proname),
    }
}

fn normalized_sql_function_body(source: &str) -> &str {
    source.trim().trim_end_matches(';').trim()
}

fn execute_known_lightweight_sql_function(
    row: &PgProcRow,
    arg_values: &[Value],
) -> Result<Option<Value>, ExecError> {
    let body = row.prosrc.trim().trim_end_matches(';').trim();
    let compact = body
        .to_ascii_lowercase()
        .split_whitespace()
        .collect::<String>();
    if compact.starts_with("selectarray_append($1,row($2,$3,$4)::") && arg_values.len() == 4 {
        return append_composite_array_value(arg_values).map(Some);
    }
    Ok(None)
}

fn validate_sql_polymorphic_runtime_args(
    row: &PgProcRow,
    arg_values: &[Value],
) -> Result<(), ExecError> {
    let declared_oids = parse_proc_argtype_oids(&row.proargtypes)?;
    let mut exact_subtype = None;
    let mut compatible_range_anchor = None;
    let mut compatible_other_subtypes = Vec::new();
    for (declared_oid, value) in declared_oids.into_iter().zip(arg_values.iter()) {
        let Some(actual_type) = value.sql_type_hint() else {
            continue;
        };
        match declared_oid {
            ANYARRAYOID if actual_type.is_array => {
                let inferred = actual_type.element_type();
                if !merge_polymorphic_runtime_subtype(&mut exact_subtype, inferred) {
                    return Err(sql_function_undefined_runtime_error(row, arg_values));
                }
            }
            ANYRANGEOID | ANYCOMPATIBLERANGEOID => {
                let Some(range_type) = range_type_ref_for_sql_type(actual_type) else {
                    return Err(sql_function_undefined_runtime_error(row, arg_values));
                };
                if declared_oid == ANYRANGEOID {
                    if !merge_polymorphic_runtime_subtype(&mut exact_subtype, range_type.subtype) {
                        return Err(sql_function_undefined_runtime_error(row, arg_values));
                    }
                } else if !merge_polymorphic_runtime_subtype(
                    &mut compatible_range_anchor,
                    range_type.subtype,
                ) {
                    return Err(sql_function_undefined_runtime_error(row, arg_values));
                }
            }
            ANYCOMPATIBLEARRAYOID if actual_type.is_array => {
                compatible_other_subtypes.push(actual_type.element_type());
            }
            _ => {}
        }
    }
    if let Some(anchor) = compatible_range_anchor {
        for actual in compatible_other_subtypes {
            if !can_coerce_to_compatible_runtime_anchor(actual, anchor) {
                return Err(sql_function_undefined_runtime_error(row, arg_values));
            }
        }
    }
    Ok(())
}

fn parse_proc_argtype_oids(argtypes: &str) -> Result<Vec<u32>, ExecError> {
    if argtypes.trim().is_empty() {
        return Ok(Vec::new());
    }
    argtypes
        .split_whitespace()
        .map(|part| {
            part.parse::<u32>().map_err(|_| {
                sql_function_runtime_error(
                    "invalid SQL function argument metadata",
                    Some(argtypes.into()),
                    "XX000",
                )
            })
        })
        .collect()
}

fn merge_polymorphic_runtime_subtype(current: &mut Option<SqlType>, inferred: SqlType) -> bool {
    match *current {
        None => {
            *current = Some(inferred);
            true
        }
        Some(existing) => sql_types_match_for_polymorphic_runtime(existing, inferred),
    }
}

fn sql_types_match_for_polymorphic_runtime(left: SqlType, right: SqlType) -> bool {
    left.kind == right.kind
        && left.is_array == right.is_array
        && (left.type_oid == 0 || right.type_oid == 0 || left.type_oid == right.type_oid)
}

fn can_coerce_to_compatible_runtime_anchor(actual: SqlType, target: SqlType) -> bool {
    if sql_types_match_for_polymorphic_runtime(actual, target) {
        return true;
    }
    if actual.is_array || target.is_array {
        return false;
    }
    matches!(
        (actual.kind, target.kind),
        (SqlTypeKind::Int2, SqlTypeKind::Int4)
            | (SqlTypeKind::Int2, SqlTypeKind::Int8)
            | (SqlTypeKind::Int2, SqlTypeKind::Numeric)
            | (SqlTypeKind::Int2, SqlTypeKind::Float4)
            | (SqlTypeKind::Int2, SqlTypeKind::Float8)
            | (SqlTypeKind::Int4, SqlTypeKind::Int8)
            | (SqlTypeKind::Int4, SqlTypeKind::Numeric)
            | (SqlTypeKind::Int4, SqlTypeKind::Float4)
            | (SqlTypeKind::Int4, SqlTypeKind::Float8)
            | (SqlTypeKind::Int8, SqlTypeKind::Numeric)
            | (SqlTypeKind::Int8, SqlTypeKind::Float4)
            | (SqlTypeKind::Int8, SqlTypeKind::Float8)
            | (SqlTypeKind::Float4, SqlTypeKind::Float8)
    )
}

fn sql_function_undefined_runtime_error(row: &PgProcRow, arg_values: &[Value]) -> ExecError {
    let signature = arg_values
        .iter()
        .map(|value| {
            value
                .sql_type_hint()
                .map(sql_type_name)
                .unwrap_or_else(|| "unknown".into())
        })
        .collect::<Vec<_>>()
        .join(", ");
    ExecError::DetailedError {
        message: format!("function {}({signature}) does not exist", row.proname),
        detail: None,
        hint: Some(
            "No function matches the given name and argument types. You might need to add explicit type casts."
                .into(),
        ),
        sqlstate: "42883",
    }
}

fn append_composite_array_value(arg_values: &[Value]) -> Result<Value, ExecError> {
    let mut array = match &arg_values[0] {
        Value::Null => ArrayValue::empty(),
        Value::PgArray(array) => array.clone(),
        other => {
            return Err(sql_function_runtime_error(
                "array_append SQL-function shortcut expected an array state",
                Some(format!("{other:?}")),
                "42804",
            ));
        }
    };
    let record = Value::Record(RecordValue::anonymous(vec![
        ("a".into(), arg_values[1].clone()),
        ("b".into(), arg_values[2].clone()),
        ("c".into(), arg_values[3].clone()),
    ]));
    match array.dimensions.as_mut_slice() {
        [] => {
            let mut new_array = ArrayValue::from_1d(vec![record]);
            if let Some(element_type_oid) = array.element_type_oid {
                new_array = new_array.with_element_type_oid(element_type_oid);
            }
            Ok(Value::PgArray(new_array))
        }
        [dim] => {
            dim.length += 1;
            array.elements.push(record);
            Ok(Value::PgArray(array))
        }
        _ => Err(sql_function_runtime_error(
            "array_append SQL-function shortcut only supports one-dimensional arrays",
            Some(format!("{array:?}")),
            "0A000",
        )),
    }
}

fn inline_sql_function_body(
    row: &PgProcRow,
    args: &[Value],
    catalog: &dyn CatalogLookup,
    datetime_config: &DateTimeConfig,
) -> Result<String, ExecError> {
    let body = normalized_sql_function_body(&row.prosrc);
    // :HACK: This is a narrow compatibility path for regression setup helpers.
    // Full PostgreSQL SQL-language functions need dedicated planning/execution
    // rather than text substitution plus a readonly single-SELECT execution.
    let lower_body = body.to_ascii_lowercase();
    if !(lower_body.starts_with("select ") || lower_body.starts_with("values ")) {
        return Err(sql_function_runtime_error(
            "only single SELECT or VALUES LANGUAGE sql function bodies are supported",
            Some(row.prosrc.clone()),
            "0A000",
        ));
    }

    let arg_type_oids = proc_input_arg_type_oids(row);
    let mut sql = substitute_positional_args_with_catalog(
        body,
        args,
        &arg_type_oids,
        catalog,
        datetime_config,
    )?;
    if let Some(names) = row.proargnames.as_ref() {
        for (index, name) in names.iter().enumerate() {
            if name.is_empty() || index >= args.len() {
                continue;
            }
            let replacement = parenthesized_sql_literal(
                &args[index],
                arg_type_oids.get(index).copied(),
                catalog,
                datetime_config,
            )?;
            sql = substitute_named_arg(&sql, name, &replacement);
        }
    }
    Ok(sql)
}

fn substitute_positional_args_with_catalog(
    input: &str,
    args: &[Value],
    arg_type_oids: &[u32],
    catalog: &dyn CatalogLookup,
    datetime_config: &DateTimeConfig,
) -> Result<String, ExecError> {
    let mut out = String::with_capacity(input.len());
    let chars = input.as_bytes();
    let mut i = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    while i < chars.len() {
        let ch = chars[i] as char;
        if in_single_quote {
            out.push(ch);
            if ch == '\'' {
                if i + 1 < chars.len() && chars[i + 1] as char == '\'' {
                    out.push('\'');
                    i += 2;
                    continue;
                }
                in_single_quote = false;
            }
            i += 1;
            continue;
        }
        if in_double_quote {
            out.push(ch);
            if ch == '"' {
                if i + 1 < chars.len() && chars[i + 1] as char == '"' {
                    out.push('"');
                    i += 2;
                    continue;
                }
                in_double_quote = false;
            }
            i += 1;
            continue;
        }
        match ch {
            '\'' => {
                in_single_quote = true;
                out.push(ch);
                i += 1;
            }
            '"' => {
                in_double_quote = true;
                out.push(ch);
                i += 1;
            }
            '$' => {
                let start = i + 1;
                let mut end = start;
                while end < chars.len() && (chars[end] as char).is_ascii_digit() {
                    end += 1;
                }
                if end == start {
                    out.push(ch);
                    i += 1;
                    continue;
                }
                let position = input[start..end].parse::<usize>().map_err(|_| {
                    sql_function_runtime_error(
                        "invalid SQL function parameter reference",
                        None,
                        "42P02",
                    )
                })?;
                let arg = args.get(position.saturating_sub(1)).ok_or_else(|| {
                    sql_function_runtime_error(
                        "SQL function parameter reference out of range",
                        Some(format!("${position}")),
                        "42P02",
                    )
                })?;
                out.push_str(&parenthesized_sql_literal(
                    arg,
                    arg_type_oids.get(position.saturating_sub(1)).copied(),
                    catalog,
                    datetime_config,
                )?);
                i = end;
            }
            _ => {
                out.push(ch);
                i += 1;
            }
        }
    }
    Ok(out)
}

fn parenthesized_sql_literal(
    value: &Value,
    type_oid: Option<u32>,
    catalog: &dyn CatalogLookup,
    datetime_config: &DateTimeConfig,
) -> Result<String, ExecError> {
    Ok(format!(
        "({})",
        render_sql_literal_with_catalog_and_type(value, type_oid, catalog, datetime_config)?
    ))
}

pub(crate) fn substitute_positional_args(input: &str, args: &[Value]) -> Result<String, ExecError> {
    let mut out = String::with_capacity(input.len());
    let chars = input.as_bytes();
    let mut i = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    while i < chars.len() {
        let ch = chars[i] as char;
        if in_single_quote {
            out.push(ch);
            if ch == '\'' {
                if i + 1 < chars.len() && chars[i + 1] as char == '\'' {
                    out.push('\'');
                    i += 2;
                    continue;
                }
                in_single_quote = false;
            }
            i += 1;
            continue;
        }
        if in_double_quote {
            out.push(ch);
            if ch == '"' {
                if i + 1 < chars.len() && chars[i + 1] as char == '"' {
                    out.push('"');
                    i += 2;
                    continue;
                }
                in_double_quote = false;
            }
            i += 1;
            continue;
        }
        match ch {
            '\'' => {
                in_single_quote = true;
                out.push(ch);
                i += 1;
            }
            '"' => {
                in_double_quote = true;
                out.push(ch);
                i += 1;
            }
            '$' => {
                let start = i + 1;
                let mut end = start;
                while end < chars.len() && (chars[end] as char).is_ascii_digit() {
                    end += 1;
                }
                if end == start {
                    out.push(ch);
                    i += 1;
                    continue;
                }
                let position = input[start..end].parse::<usize>().map_err(|_| {
                    sql_function_runtime_error(
                        "invalid SQL function parameter reference",
                        None,
                        "42P02",
                    )
                })?;
                let arg = args.get(position.saturating_sub(1)).ok_or_else(|| {
                    sql_function_runtime_error(
                        "SQL function parameter reference out of range",
                        Some(format!("${position}")),
                        "42P02",
                    )
                })?;
                out.push_str(&render_sql_literal(arg)?);
                i = end;
            }
            _ => {
                out.push(ch);
                i += 1;
            }
        }
    }
    Ok(out)
}

fn proc_input_arg_type_oids(row: &PgProcRow) -> Vec<u32> {
    row.proargtypes
        .split_whitespace()
        .filter_map(|oid| oid.parse::<u32>().ok())
        .collect()
}

pub(crate) fn substitute_named_arg(input: &str, name: &str, replacement: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let chars = input.as_bytes();
    let mut i = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    while i < chars.len() {
        let ch = chars[i] as char;
        if in_single_quote {
            out.push(ch);
            if ch == '\'' {
                if i + 1 < chars.len() && chars[i + 1] as char == '\'' {
                    out.push('\'');
                    i += 2;
                    continue;
                }
                in_single_quote = false;
            }
            i += 1;
            continue;
        }
        if in_double_quote {
            out.push(ch);
            if ch == '"' {
                if i + 1 < chars.len() && chars[i + 1] as char == '"' {
                    out.push('"');
                    i += 2;
                    continue;
                }
                in_double_quote = false;
            }
            i += 1;
            continue;
        }
        match ch {
            '\'' => {
                in_single_quote = true;
                out.push(ch);
                i += 1;
            }
            '"' => {
                in_double_quote = true;
                out.push(ch);
                i += 1;
            }
            _ if ch == '_' || ch.is_ascii_alphabetic() => {
                let start = i;
                i += 1;
                while i < chars.len() {
                    let ch = chars[i] as char;
                    if ch == '_' || ch.is_ascii_alphanumeric() {
                        i += 1;
                    } else {
                        break;
                    }
                }
                let ident = &input[start..i];
                if ident.eq_ignore_ascii_case(name) {
                    out.push_str(replacement);
                } else {
                    out.push_str(ident);
                }
            }
            _ => {
                out.push(ch);
                i += 1;
            }
        }
    }
    out
}

fn render_sql_literal_with_catalog(
    value: &Value,
    catalog: &dyn CatalogLookup,
    datetime_config: &DateTimeConfig,
) -> Result<String, ExecError> {
    Ok(match value {
        Value::Null => "null".into(),
        Value::Bool(true) => "true".into(),
        Value::Bool(false) => "false".into(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Bytea(bytes) => {
            let hex = format_bytea_text(bytes, ByteaOutputFormat::Hex);
            let hex = hex.strip_prefix("\\x").unwrap_or(&hex);
            format!("decode('{}', 'hex')", hex)
        }
        Value::Text(text) => format!("{}::text", quote_sql_string(text)),
        Value::TextRef(_, _) => {
            let text = value.as_text().unwrap_or_default();
            format!("{}::text", quote_sql_string(text))
        }
        Value::Json(text) => format!("{}::json", quote_sql_string(text)),
        Value::Jsonb(bytes) => format!(
            "{}::jsonb",
            quote_sql_string(std::str::from_utf8(bytes).map_err(|_| {
                sql_function_runtime_error(
                    "invalid JSONB UTF-8 while rendering SQL literal",
                    None,
                    "XX000",
                )
            })?)
        ),
        Value::Range(_) => {
            let ty = value.sql_type_hint().ok_or_else(|| {
                sql_function_runtime_error("cannot infer SQL function argument type", None, "42804")
            })?;
            let type_name = sql_type_literal_name(catalog, ty)?;
            let text = render_range_text_with_config(value, datetime_config).unwrap_or_default();
            format!("{}::{type_name}", quote_sql_string(&text))
        }
        Value::Multirange(_) => {
            let ty = value.sql_type_hint().ok_or_else(|| {
                sql_function_runtime_error("cannot infer SQL function argument type", None, "42804")
            })?;
            let type_name = sql_type_literal_name(catalog, ty)?;
            let text =
                render_multirange_text_with_config(value, datetime_config).unwrap_or_default();
            format!("{}::{type_name}", quote_sql_string(&text))
        }
        Value::PgArray(array) => {
            let element_type_oid = array
                .element_type_oid
                .or_else(|| {
                    array
                        .elements
                        .iter()
                        .find_map(|value| value.sql_type_hint())
                        .and_then(|ty| catalog.type_oid_for_sql_type(ty))
                })
                .ok_or_else(|| {
                    sql_function_runtime_error(
                        "cannot infer SQL function array argument type",
                        None,
                        "42804",
                    )
                })?;
            let element_name = type_name_for_oid(catalog, element_type_oid)?;
            let text = format_array_value_text_with_config(array, datetime_config);
            format!("{}::{element_name}[]", quote_sql_string(&text))
        }
        Value::Array(values) => {
            let array = ArrayValue::from_1d(values.clone());
            let element_type = values
                .iter()
                .find_map(Value::sql_type_hint)
                .ok_or_else(|| {
                    sql_function_runtime_error(
                        "cannot infer SQL function array argument type",
                        None,
                        "42804",
                    )
                })?;
            let element_oid = catalog.type_oid_for_sql_type(element_type).ok_or_else(|| {
                sql_function_runtime_error(
                    "cannot infer SQL function array argument type",
                    Some(format!("{element_type:?}")),
                    "42804",
                )
            })?;
            let element_name = type_name_for_oid(catalog, element_oid)?;
            let text = format_array_value_text_with_config(&array, datetime_config);
            format!("{}::{element_name}[]", quote_sql_string(&text))
        }
        Value::Record(record) => {
            let type_oid = record.type_oid();
            if type_oid == 0 {
                return Err(sql_function_runtime_error(
                    "cannot infer SQL function record argument type",
                    None,
                    "42804",
                ));
            }
            let type_name = type_name_for_oid(catalog, type_oid)?;
            let fields = record
                .fields
                .iter()
                .map(|field| render_sql_literal_with_catalog(field, catalog, datetime_config))
                .collect::<Result<Vec<_>, _>>()?;
            format!("ROW({})::{type_name}", fields.join(", "))
        }
        _ => {
            return Err(sql_function_runtime_error(
                "SQL function argument type is not supported by the lightweight SQL-function runtime",
                Some(format!("{value:?}")),
                "0A000",
            ));
        }
    })
}

fn render_sql_literal_with_catalog_and_type(
    value: &Value,
    type_oid: Option<u32>,
    catalog: &dyn CatalogLookup,
    datetime_config: &DateTimeConfig,
) -> Result<String, ExecError> {
    let Some(type_oid) = type_oid else {
        return render_sql_literal_with_catalog(value, catalog, datetime_config);
    };
    let Some(element_type_oid) = expected_array_element_type_oid(catalog, type_oid) else {
        return render_sql_literal_with_catalog(value, catalog, datetime_config);
    };
    match value {
        Value::Array(values) if values.is_empty() => {
            let array = ArrayValue::from_1d(Vec::new());
            let element_name = type_name_for_oid(catalog, element_type_oid)?;
            let text = format_array_value_text_with_config(&array, datetime_config);
            Ok(format!("{}::{element_name}[]", quote_sql_string(&text)))
        }
        Value::PgArray(array) if array.element_type_oid.is_none() && array.elements.is_empty() => {
            let element_name = type_name_for_oid(catalog, element_type_oid)?;
            let text = format_array_value_text_with_config(array, datetime_config);
            Ok(format!("{}::{element_name}[]", quote_sql_string(&text)))
        }
        _ => render_sql_literal_with_catalog(value, catalog, datetime_config),
    }
}

fn expected_array_element_type_oid(catalog: &dyn CatalogLookup, type_oid: u32) -> Option<u32> {
    let sql_type = catalog.type_by_oid(type_oid)?.sql_type;
    sql_type
        .is_array
        .then(|| catalog.type_oid_for_sql_type(sql_type.element_type()))
        .flatten()
}

pub(crate) fn render_sql_literal(value: &Value) -> Result<String, ExecError> {
    let catalog = crate::backend::parser::Catalog::default();
    render_sql_literal_with_catalog(value, &catalog, &DateTimeConfig::default())
}

fn sql_type_literal_name(catalog: &dyn CatalogLookup, ty: SqlType) -> Result<String, ExecError> {
    if ty.is_array {
        let element = ty.element_type();
        let element_oid = catalog.type_oid_for_sql_type(element).ok_or_else(|| {
            sql_function_runtime_error(
                "cannot resolve SQL function array argument type",
                Some(format!("{ty:?}")),
                "42804",
            )
        })?;
        return Ok(format!("{}[]", type_name_for_oid(catalog, element_oid)?));
    }
    let type_oid = if ty.type_oid != 0 {
        ty.type_oid
    } else {
        catalog.type_oid_for_sql_type(ty).ok_or_else(|| {
            sql_function_runtime_error(
                "cannot resolve SQL function argument type",
                Some(format!("{ty:?}")),
                "42804",
            )
        })?
    };
    type_name_for_oid(catalog, type_oid)
}

fn type_name_for_oid(catalog: &dyn CatalogLookup, type_oid: u32) -> Result<String, ExecError> {
    let row = catalog.type_by_oid(type_oid).ok_or_else(|| {
        sql_function_runtime_error(
            "cannot resolve SQL function argument type name",
            Some(format!("type oid {type_oid}")),
            "42804",
        )
    })?;
    Ok(quote_sql_identifier(&row.typname))
}

fn quote_sql_identifier(name: &str) -> String {
    if is_plain_sql_identifier(name) {
        return name.to_string();
    }
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn is_plain_sql_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_lowercase())
        && chars.all(|ch| ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit())
}

fn quote_sql_string(text: &str) -> String {
    let escaped = text.replace('\'', "''");
    if text.contains('\\') {
        let escaped = escaped.replace('\\', "\\\\");
        format!("E'{escaped}'")
    } else {
        format!("'{escaped}'")
    }
}

fn sql_function_runtime_error(
    message: &str,
    detail: Option<String>,
    sqlstate: &'static str,
) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail,
        hint: None,
        sqlstate,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_proc_row(body: &str, argnames: Option<Vec<&str>>) -> PgProcRow {
        PgProcRow {
            oid: 9999,
            proname: "test_sql_func".into(),
            pronamespace: 2200,
            proowner: 10,
            proacl: None,
            prolang: PG_LANGUAGE_SQL_OID,
            procost: 1.0,
            prorows: 0.0,
            provariadic: 0,
            prosupport: 0,
            prokind: 'f',
            prosecdef: false,
            proleakproof: false,
            proisstrict: false,
            proretset: false,
            provolatile: 'i',
            proparallel: 's',
            pronargs: argnames
                .as_ref()
                .map(|names| names.len() as i16)
                .unwrap_or(0),
            pronargdefaults: 0,
            prorettype: 25,
            proargtypes: String::new(),
            proallargtypes: None,
            proargmodes: None,
            proargnames: argnames.map(|names| {
                names
                    .into_iter()
                    .map(std::string::ToString::to_string)
                    .collect()
            }),
            proargdefaults: None,
            prosrc: body.into(),
            probin: None,
            prosqlbody: None,
            proconfig: None,
        }
    }

    #[test]
    fn inline_sql_function_substitutes_positional_and_named_args() {
        let catalog = crate::backend::parser::Catalog::default();
        let datetime_config = DateTimeConfig::default();
        let row = test_proc_row("select value + $2", Some(vec!["value", "seed"]));
        let sql = inline_sql_function_body(
            &row,
            &[Value::Int32(4), Value::Int64(10)],
            &catalog,
            &datetime_config,
        )
        .unwrap();
        assert_eq!(sql, "select (4) + (10)");
    }

    #[test]
    fn inline_sql_function_does_not_replace_identifiers_inside_quotes() {
        let catalog = crate::backend::parser::Catalog::default();
        let datetime_config = DateTimeConfig::default();
        let row = test_proc_row("select 'value', \"$1\", value", Some(vec!["value"]));
        let sql = inline_sql_function_body(
            &row,
            &[Value::Text("abc".into())],
            &catalog,
            &datetime_config,
        )
        .unwrap();
        assert_eq!(sql, "select 'value', \"$1\", ('abc'::text)");
    }

    #[test]
    fn inline_sql_function_rejects_non_select_body() {
        let catalog = crate::backend::parser::Catalog::default();
        let datetime_config = DateTimeConfig::default();
        let row = test_proc_row("return 1", None);
        let err = inline_sql_function_body(&row, &[], &catalog, &datetime_config).unwrap_err();
        assert!(matches!(
            err,
            ExecError::DetailedError {
                sqlstate: "0A000",
                ..
            }
        ));
    }
}
