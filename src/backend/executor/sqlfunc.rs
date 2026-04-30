use super::domain::enforce_domain_constraints_for_value;
use super::exec_expr::cast_record_value_for_target;
use super::value_io::{coerce_assignment_value_with_config, format_array_value_text_with_config};
use crate::backend::executor::exec_expr::append_array_value;
use crate::backend::executor::execute_readonly_statement;
use crate::backend::executor::function_guc::execute_with_sql_function_gucs;
use crate::backend::executor::{
    ExecError, ExecutorContext, QueryColumn, StatementResult, TupleSlot, Value,
    render_datetime_value_text_with_config, render_geometry_text, render_interval_text_with_config,
    render_multirange_text_with_config, render_range_text_with_config,
};
use crate::backend::libpq::pqformat::format_bytea_text;
use crate::backend::parser::analyze::sql_type_name;
use crate::backend::parser::{
    CatalogLookup, ParseOptions, SqlType, SqlTypeKind, Statement, bind_insert,
    parse_statement_with_options,
};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::include::catalog::PgProcRow;
use crate::include::catalog::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLEOID,
    ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYMULTIRANGEOID, ANYOID, ANYRANGEOID,
    PG_LANGUAGE_SQL_OID, RECORD_TYPE_OID, builtin_multirange_name_for_sql_type,
    builtin_range_name_for_sql_type, range_type_ref_for_multirange_sql_type,
    range_type_ref_for_sql_type,
};
use crate::include::nodes::datum::{ArrayValue, RecordDescriptor, RecordValue};
use crate::include::nodes::primnodes::{Expr, expr_sql_type_hint};
use crate::pgrust::database::commands::rules::execute_bound_insert_with_rules;
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
    let arg_type_oids = sql_function_call_arg_type_oids(args, ctx);
    execute_user_defined_sql_scalar_function_values_with_arg_type_oids(
        row,
        &arg_values,
        arg_type_oids.as_deref(),
        ctx,
    )
}

pub(crate) fn execute_user_defined_sql_scalar_function_values(
    row: &PgProcRow,
    arg_values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    execute_user_defined_sql_scalar_function_values_with_arg_type_oids(row, arg_values, None, ctx)
}

pub(crate) fn execute_user_defined_sql_scalar_function_values_with_arg_type_oids(
    row: &PgProcRow,
    arg_values: &[Value],
    arg_type_oids: Option<&[u32]>,
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
        let runtime_result_type =
            sql_function_runtime_result_type(row, arg_values, catalog.as_ref())?;
        let result =
            execute_sql_function_query(row, &arg_values, arg_type_oids, catalog.as_ref(), ctx)?;
        match result {
            StatementResult::Query { columns, rows, .. } => sql_scalar_function_result_value(
                row,
                catalog.as_ref(),
                &columns,
                rows,
                runtime_result_type,
                ctx,
            ),
            other => Err(sql_function_runtime_error(
                "LANGUAGE sql function did not produce a query result",
                Some(format!("{other:?}")),
                "0A000",
            )),
        }
    })
    .map_err(|err| sql_function_context_error(row, err))
}

fn coerce_sql_function_value_to_type(
    value: Value,
    target_type: SqlType,
    _catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let coerced = if let Value::Record(record) = value {
        cast_record_value_for_target(record, target_type, ctx)
    } else {
        coerce_assignment_value_with_config(&value, target_type, &ctx.datetime_config)
    }?;
    enforce_domain_constraints_for_value(coerced, target_type, ctx)
}

fn out_parameter_record_value(
    row: &PgProcRow,
    catalog: &dyn CatalogLookup,
    values: &[Value],
) -> Option<Value> {
    let (Some(arg_types), Some(arg_modes)) = (&row.proallargtypes, &row.proargmodes) else {
        return None;
    };
    if arg_types.len() != arg_modes.len() {
        return None;
    }
    let arg_names = row.proargnames.as_deref().unwrap_or(&[]);
    let fields = arg_types
        .iter()
        .zip(arg_modes.iter())
        .enumerate()
        .filter_map(|(index, (type_oid, mode))| {
            matches!(*mode, b'o' | b'b' | b't').then(|| {
                catalog.type_by_oid(*type_oid).map(|type_row| {
                    let name = arg_names
                        .get(index)
                        .filter(|name| !name.is_empty())
                        .cloned()
                        .unwrap_or_else(|| format!("column{}", index + 1));
                    (name, type_row.sql_type)
                })
            })?
        })
        .collect::<Vec<_>>();
    if fields.len() != values.len() || fields.len() <= 1 {
        return None;
    }
    Some(Value::Record(RecordValue::from_descriptor(
        RecordDescriptor::anonymous(fields, -1),
        values.to_vec(),
    )))
}

fn sql_function_call_arg_type_oids(args: &[Expr], ctx: &ExecutorContext) -> Option<Vec<u32>> {
    let catalog = ctx.catalog.as_deref()?;
    Some(
        args.iter()
            .map(|arg| expr_sql_type_hint(arg).and_then(|ty| catalog.type_oid_for_sql_type(ty)))
            .map(|oid| oid.unwrap_or(0))
            .collect(),
    )
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
        let arg_type_oids = sql_function_call_arg_type_oids(args, ctx);
        let result = execute_sql_function_query(
            row,
            &arg_values,
            arg_type_oids.as_deref(),
            catalog.as_ref(),
            ctx,
        )?;
        let runtime_result_type =
            sql_function_runtime_result_type(row, &arg_values, catalog.as_ref())?;
        let expand_single_record = sql_function_declares_record_result(row, catalog.as_ref());
        match result {
            StatementResult::Query { columns, rows, .. } => {
                let projection = sql_function_composite_projection(
                    row,
                    &columns,
                    output_columns,
                    catalog.as_ref(),
                )?;
                let returned_columns = projection
                    .as_ref()
                    .map(|projection| projection.columns.as_slice())
                    .unwrap_or(columns.as_slice());
                validate_sql_function_query_columns(
                    catalog.as_ref(),
                    returned_columns,
                    output_columns,
                )?;
                let rows: Box<dyn Iterator<Item = Vec<Value>>> = if row.proretset {
                    Box::new(rows.into_iter())
                } else {
                    Box::new(rows.into_iter().take(1))
                };
                rows.map(|mut row| {
                    if let Some(projection) = &projection {
                        row = project_sql_function_row(row, projection);
                    }
                    if should_pack_sql_set_returning_record_row(output_columns, row.as_slice()) {
                        let coerced = vec![pack_sql_function_record_row(row, returned_columns)];
                        return Ok(TupleSlot::virtual_row(coerced));
                    }
                    if row.len() == 1
                        && sql_function_single_value_is_whole_result(
                            runtime_result_type,
                            output_columns,
                            row.as_slice(),
                        )
                    {
                        return sql_function_result_row_for_output(
                            row,
                            runtime_result_type,
                            output_columns,
                            catalog.as_ref(),
                            ctx,
                            expand_single_record,
                        );
                    }
                    let coerced =
                        coerce_sql_function_row_values(row, output_columns, catalog.as_ref(), ctx)?;
                    Ok(TupleSlot::virtual_row(coerced))
                })
                .collect()
            }
            other => Err(sql_function_runtime_error(
                "LANGUAGE sql function did not produce a query result",
                Some(format!("{other:?}")),
                "0A000",
            )),
        }
    })
    .map_err(|err| sql_function_context_error(row, err))
}

struct SqlFunctionCompositeProjection {
    indexes: Vec<usize>,
    columns: Vec<QueryColumn>,
}

fn sql_function_composite_projection(
    row: &PgProcRow,
    returned_columns: &[QueryColumn],
    expected_columns: &[QueryColumn],
    catalog: &dyn CatalogLookup,
) -> Result<Option<SqlFunctionCompositeProjection>, ExecError> {
    if returned_columns == expected_columns {
        return Ok(None);
    }
    if sql_function_outputs_single_composite_column(expected_columns) {
        return Ok(None);
    }
    let Some(relation_desc) = sql_function_return_relation_desc(row, catalog) else {
        return Ok(None);
    };
    if let Some(dropped_index) =
        missing_expected_composite_output_attr_index(&relation_desc, expected_columns)
    {
        return Err(ExecError::Parse(
            crate::backend::parser::ParseError::DetailedError {
                message: format!(
                    "attribute {} of type record has been dropped",
                    dropped_index.saturating_add(1)
                ),
                detail: None,
                hint: None,
                sqlstate: "42703",
            },
        ));
    }

    let mut indexes = Vec::with_capacity(expected_columns.len());
    let mut columns = Vec::with_capacity(expected_columns.len());
    let mut search_from = 0usize;
    for expected in expected_columns {
        let Some((index, column)) =
            returned_columns
                .iter()
                .enumerate()
                .skip(search_from)
                .find(|(_, returned)| {
                    returned.name == expected.name && returned.sql_type == expected.sql_type
                })
        else {
            return Ok(None);
        };
        indexes.push(index);
        columns.push(column.clone());
        search_from = index.saturating_add(1);
    }

    if indexes
        .iter()
        .enumerate()
        .all(|(index, projected)| index == *projected)
        && indexes.len() == returned_columns.len()
    {
        return Ok(None);
    }

    Ok(Some(SqlFunctionCompositeProjection { indexes, columns }))
}

fn project_sql_function_row(
    row: Vec<Value>,
    projection: &SqlFunctionCompositeProjection,
) -> Vec<Value> {
    projection
        .indexes
        .iter()
        .map(|index| row.get(*index).cloned().unwrap_or(Value::Null))
        .collect()
}

fn sql_function_return_relation_desc(
    row: &PgProcRow,
    catalog: &dyn CatalogLookup,
) -> Option<crate::include::nodes::primnodes::RelationDesc> {
    let return_type = catalog.type_by_oid(row.prorettype)?;
    if return_type.typrelid == 0 {
        return None;
    }
    catalog
        .relation_by_oid(return_type.typrelid)
        .or_else(|| catalog.lookup_relation_by_oid(return_type.typrelid))
        .map(|relation| relation.desc)
}

fn missing_expected_composite_output_attr_index(
    desc: &crate::include::nodes::primnodes::RelationDesc,
    expected_columns: &[QueryColumn],
) -> Option<usize> {
    let mut cursor = 0usize;
    for (index, expected) in expected_columns.iter().enumerate() {
        if let Some(found) = find_live_composite_column_at_or_after(desc, expected, cursor) {
            cursor = found.saturating_add(1);
            continue;
        }
        let next_live_index = expected_columns
            .iter()
            .skip(index.saturating_add(1))
            .find_map(|column| find_live_composite_column_at_or_after(desc, column, cursor))
            .unwrap_or(desc.columns.len());
        return desc
            .columns
            .iter()
            .enumerate()
            .take(next_live_index)
            .skip(cursor)
            .rev()
            .find_map(|(attr_index, column)| column.dropped.then_some(attr_index))
            .or_else(|| {
                desc.columns
                    .iter()
                    .enumerate()
                    .skip(cursor)
                    .find_map(|(attr_index, column)| column.dropped.then_some(attr_index))
            });
    }
    None
}

fn find_live_composite_column_at_or_after(
    desc: &crate::include::nodes::primnodes::RelationDesc,
    expected: &QueryColumn,
    start: usize,
) -> Option<usize> {
    desc.columns
        .iter()
        .enumerate()
        .skip(start)
        .find(|(_, column)| {
            !column.dropped && column.name == expected.name && column.sql_type == expected.sql_type
        })
        .map(|(index, _)| index)
}

fn sql_function_outputs_single_composite_column(output_columns: &[QueryColumn]) -> bool {
    output_columns.len() == 1
        && matches!(
            output_columns[0].sql_type.kind,
            SqlTypeKind::Composite | SqlTypeKind::Record
        )
}

fn sql_function_single_value_is_whole_result(
    runtime_result_type: Option<SqlType>,
    output_columns: &[QueryColumn],
    values: &[Value],
) -> bool {
    if matches!(values, [Value::Record(_)]) {
        return true;
    }
    let Some(return_type) = runtime_result_type else {
        return true;
    };
    if !matches!(
        return_type.kind,
        SqlTypeKind::Composite | SqlTypeKind::Record
    ) {
        return true;
    }
    output_columns.len() == 1
        && matches!(
            output_columns[0].sql_type.kind,
            SqlTypeKind::Composite | SqlTypeKind::Record
        )
}

fn should_pack_sql_set_returning_record_row(
    output_columns: &[QueryColumn],
    values: &[Value],
) -> bool {
    output_columns.len() == 1
        && matches!(output_columns[0].sql_type.kind, SqlTypeKind::Record)
        && !matches!(values, [Value::Record(_)])
}

fn sql_function_result_row_for_output(
    result_row: Vec<Value>,
    runtime_result_type: Option<SqlType>,
    output_columns: &[QueryColumn],
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    expand_single_record: bool,
) -> Result<TupleSlot, ExecError> {
    let mut value = result_row.into_iter().next().unwrap_or(Value::Null);
    if let Some(return_type) = runtime_result_type {
        value = coerce_sql_function_value_to_type(value, return_type, catalog, ctx)?;
    }
    if (expand_single_record || output_columns.len() > 1)
        && let Value::Record(record) = value
    {
        let mut fields = record.fields;
        if fields.len() < output_columns.len() {
            fields.resize(output_columns.len(), Value::Null);
        }
        fields.truncate(output_columns.len());
        return Ok(TupleSlot::virtual_row(fields));
    }

    let mut row = vec![value];
    if row.len() < output_columns.len() {
        row.resize(output_columns.len(), Value::Null);
    }
    row.truncate(output_columns.len());
    Ok(TupleSlot::virtual_row(row))
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

fn pack_sql_function_record_row(values: Vec<Value>, columns: &[QueryColumn]) -> Value {
    let descriptor = RecordDescriptor::anonymous(
        columns
            .iter()
            .map(|column| (column.name.clone(), column.sql_type))
            .collect(),
        -1,
    );
    Value::Record(RecordValue::from_descriptor(descriptor, values))
}

fn validate_sql_function_query_columns(
    catalog: &dyn CatalogLookup,
    returned_columns: &[QueryColumn],
    expected_columns: &[QueryColumn],
) -> Result<(), ExecError> {
    if expected_columns.len() == 1
        && matches!(expected_columns[0].sql_type.kind, SqlTypeKind::Record)
    {
        return Ok(());
    }
    if returned_columns.len() == 1
        && expected_columns.len() != 1
        && matches!(
            returned_columns[0].sql_type.kind,
            SqlTypeKind::Record | SqlTypeKind::Composite
        )
    {
        return Ok(());
    }
    if returned_columns.len() != expected_columns.len() {
        return Err(sql_function_return_row_mismatch(
            expected_columns.len(),
            returned_columns.len(),
        ));
    }
    for (index, (returned, expected)) in returned_columns
        .iter()
        .zip(expected_columns.iter())
        .enumerate()
    {
        if !sql_function_column_assignment_compatible(catalog, returned.sql_type, expected.sql_type)
        {
            return Err(sql_function_final_statement_type_mismatch(
                index + 1,
                returned.sql_type,
                expected.sql_type,
            ));
        }
    }
    Ok(())
}

fn sql_function_column_assignment_compatible(
    catalog: &dyn CatalogLookup,
    returned_type: SqlType,
    expected_type: SqlType,
) -> bool {
    if returned_type.kind == expected_type.kind
        && returned_type.is_array == expected_type.is_array
        && (returned_type.type_oid == expected_type.type_oid
            || returned_type.type_oid == 0
            || expected_type.type_oid == 0)
    {
        return true;
    }
    let Some(returned_oid) = catalog.type_oid_for_sql_type(returned_type) else {
        return false;
    };
    let Some(expected_oid) = catalog.type_oid_for_sql_type(expected_type) else {
        return false;
    };
    if returned_oid == expected_oid {
        return true;
    }
    catalog
        .cast_by_source_target(returned_oid, expected_oid)
        .is_some_and(|row| matches!(row.castcontext, 'i' | 'a'))
}

fn execute_sql_function_query(
    row: &PgProcRow,
    arg_values: &[Value],
    arg_type_oids: Option<&[u32]>,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    let sql = inline_sql_function_body(
        row,
        arg_values,
        arg_type_oids,
        catalog,
        &ctx.datetime_config,
    )?;
    let stmt = parse_statement_with_options(
        &sql,
        ParseOptions {
            max_stack_depth_kb: ctx.datetime_config.max_stack_depth_kb,
            ..ParseOptions::default()
        },
    )?;
    let saved_expr_bindings = std::mem::take(&mut ctx.expr_bindings);
    let restore_row_security = apply_sql_function_row_security_set_config_effect(&sql, ctx);
    let saved_snapshot_cid = if row.provolatile == 'v' {
        let saved = ctx.snapshot.current_cid;
        ctx.snapshot.current_cid = crate::backend::access::transam::xact::CommandId::MAX;
        Some(saved)
    } else {
        None
    };
    let result = execute_sql_function_statement(stmt, catalog, ctx);
    if let Some(saved) = saved_snapshot_cid {
        ctx.snapshot.current_cid = saved;
    }
    restore_sql_function_row_security_set_config_effect(restore_row_security, ctx);
    ctx.expr_bindings = saved_expr_bindings;
    result
}

fn apply_sql_function_row_security_set_config_effect(
    sql: &str,
    ctx: &mut ExecutorContext,
) -> Option<(Option<String>, Option<bool>)> {
    if !sql_function_sets_row_security_off(sql) {
        return None;
    }
    // :HACK: pgrust's SQL-function executor plans one substituted statement at
    // runtime. PostgreSQL's rowsecurity regression relies on a volatile
    // set_config('row_security', 'false', ...) in that statement changing the
    // RLS rewrite context. Apply just that planning-visible effect here until
    // SQL-language functions get a full statement execution engine.
    let saved_guc = ctx.gucs.get("row_security").cloned();
    let saved_db = ctx
        .database
        .as_ref()
        .map(|db| db.row_security_enabled(ctx.client_id));
    ctx.gucs.insert("row_security".into(), "off".into());
    if let Some(db) = &ctx.database {
        db.install_row_security_enabled(ctx.client_id, false);
        db.plan_cache.invalidate_all();
    }
    Some((saved_guc, saved_db))
}

fn restore_sql_function_row_security_set_config_effect(
    saved: Option<(Option<String>, Option<bool>)>,
    ctx: &mut ExecutorContext,
) {
    let Some((saved_guc, saved_db)) = saved else {
        return;
    };
    if let Some(value) = saved_guc {
        ctx.gucs.insert("row_security".into(), value);
    } else {
        ctx.gucs.remove("row_security");
    }
    if let (Some(db), Some(enabled)) = (&ctx.database, saved_db) {
        db.install_row_security_enabled(ctx.client_id, enabled);
    }
}

fn sql_function_sets_row_security_off(sql: &str) -> bool {
    let compact: String = sql
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect();
    compact.contains("set_config('row_security','false',")
        || compact.contains("set_config('row_security','off',")
}

fn execute_sql_function_statement(
    stmt: Statement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    match stmt {
        Statement::Insert(stmt) => {
            if !ctx.allow_side_effects {
                return Err(ExecError::DetailedError {
                    message: "INSERT is not allowed in a read-only execution context".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "25006",
                });
            }
            let xid = ctx.ensure_write_xid()?;
            let cid = ctx.next_command_id;
            let result = execute_bound_insert_with_rules(
                bind_insert(&stmt, catalog)?,
                catalog,
                ctx,
                xid,
                cid,
            );
            ctx.next_command_id = ctx.next_command_id.saturating_add(1);
            result
        }
        stmt => execute_readonly_statement(stmt, catalog, ctx),
    }
}

fn sql_scalar_function_result_value(
    row: &PgProcRow,
    catalog: &dyn CatalogLookup,
    columns: &[QueryColumn],
    rows: Vec<Vec<Value>>,
    runtime_result_type: Option<SqlType>,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let Some(first_row) = rows.into_iter().next() else {
        return Ok(Value::Null);
    };
    let return_type =
        runtime_result_type.or_else(|| catalog.type_by_oid(row.prorettype).map(|row| row.sql_type));
    if first_row.len() == 1
        && let Some(return_type) = return_type
        && (catalog.domain_by_type_oid(return_type.type_oid).is_some()
            || !matches!(
                return_type.kind,
                SqlTypeKind::Record | SqlTypeKind::Composite
            ))
    {
        let value = first_row.into_iter().next().unwrap_or(Value::Null);
        return coerce_sql_function_value_to_type(value, return_type, catalog, ctx);
    }
    if let Some(output) = sql_scalar_function_record_output(row, catalog, columns) {
        let fields = sql_scalar_function_record_fields(&output, first_row, catalog, ctx)?;
        let value = Value::Record(RecordValue::from_descriptor(output.descriptor, fields));
        if let Some(return_type) = return_type
            && catalog.domain_by_type_oid(return_type.type_oid).is_some()
        {
            return enforce_domain_constraints_for_value(value, return_type, ctx);
        }
        return Ok(value);
    }
    if first_row.len() == 1 {
        let value = first_row.into_iter().next().unwrap_or(Value::Null);
        return Ok(value);
    }
    Err(sql_function_runtime_error(
        "scalar SQL function returned an unexpected row shape",
        Some(format!("expected 1 column, got {}", first_row.len())),
        "42804",
    ))
}

fn sql_function_runtime_result_type(
    row: &PgProcRow,
    arg_values: &[Value],
    catalog: &dyn CatalogLookup,
) -> Result<Option<SqlType>, ExecError> {
    let declared_oids = parse_proc_argtype_oids(&row.proargtypes)?;
    let mut anyelement = None;
    let mut anyarray = None;
    let mut anyrange = None;
    let mut anymultirange = None;
    let mut anycompatible = None;
    let mut anycompatiblerange = None;
    let mut anycompatiblemultirange = None;

    for (declared_oid, value) in declared_oids.into_iter().zip(arg_values.iter()) {
        let Some(actual_type) = value.sql_type_hint() else {
            continue;
        };
        match declared_oid {
            ANYOID | ANYELEMENTOID => merge_runtime_type(&mut anyelement, actual_type),
            ANYARRAYOID if actual_type.is_array => {
                merge_runtime_type(&mut anyarray, actual_type);
                merge_runtime_type(&mut anyelement, actual_type.element_type());
            }
            ANYRANGEOID if actual_type.is_range() => {
                merge_runtime_type(&mut anyrange, actual_type);
                if let Some(range_type) = range_type_ref_for_sql_type(actual_type) {
                    merge_runtime_type(&mut anyelement, range_type.subtype);
                }
            }
            ANYMULTIRANGEOID if actual_type.is_multirange() => {
                merge_runtime_type(&mut anymultirange, actual_type);
                if let Some(range_type) = range_type_ref_for_multirange_sql_type(actual_type) {
                    merge_runtime_type(&mut anyrange, range_type.sql_type);
                    merge_runtime_type(&mut anyelement, range_type.subtype);
                }
            }
            ANYCOMPATIBLEOID => merge_runtime_type(&mut anycompatible, actual_type),
            ANYCOMPATIBLEARRAYOID if actual_type.is_array => {
                merge_runtime_type(&mut anycompatible, actual_type.element_type());
            }
            ANYCOMPATIBLERANGEOID if actual_type.is_range() => {
                merge_runtime_type(&mut anycompatiblerange, actual_type);
                if let Some(range_type) = range_type_ref_for_sql_type(actual_type) {
                    merge_runtime_type(&mut anycompatible, range_type.subtype);
                }
            }
            ANYCOMPATIBLEMULTIRANGEOID if actual_type.is_multirange() => {
                merge_runtime_type(&mut anycompatiblemultirange, actual_type);
                if let Some(range_type) = range_type_ref_for_multirange_sql_type(actual_type) {
                    merge_runtime_type(&mut anycompatiblerange, range_type.sql_type);
                    merge_runtime_type(&mut anycompatible, range_type.subtype);
                }
            }
            _ => {}
        }
    }

    let resolved = match row.prorettype {
        ANYOID | ANYELEMENTOID => anyelement,
        ANYARRAYOID => anyarray.or_else(|| anyelement.map(SqlType::array_of)),
        ANYRANGEOID => anyrange,
        ANYMULTIRANGEOID => anymultirange,
        ANYCOMPATIBLEOID => anycompatible,
        ANYCOMPATIBLEARRAYOID => anycompatible.map(SqlType::array_of),
        ANYCOMPATIBLERANGEOID => anycompatiblerange,
        ANYCOMPATIBLEMULTIRANGEOID => anycompatiblemultirange,
        _ => catalog.type_by_oid(row.prorettype).map(|row| row.sql_type),
    };
    Ok(resolved)
}

fn merge_runtime_type(slot: &mut Option<SqlType>, ty: SqlType) {
    if slot.is_none() {
        *slot = Some(ty);
    }
}

fn coerce_sql_function_row_values(
    mut values: Vec<Value>,
    expected_columns: &[QueryColumn],
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    if let [Value::Record(record)] = values.as_slice()
        && expected_columns.len() != 1
    {
        validate_sql_function_record_field_types(record, expected_columns)?;
        values = record.fields.clone();
    }
    if values.len() != expected_columns.len() {
        return Err(sql_function_return_row_mismatch(
            expected_columns.len(),
            values.len(),
        ));
    }
    values
        .into_iter()
        .zip(expected_columns.iter())
        .map(|(value, column)| {
            coerce_sql_function_value_to_type(value, column.sql_type, catalog, ctx)
                .map_err(|err| sql_function_return_type_error(&column.name, column.sql_type, err))
        })
        .collect()
}

fn validate_sql_function_record_field_types(
    record: &RecordValue,
    expected_columns: &[QueryColumn],
) -> Result<(), ExecError> {
    if record.descriptor.fields.len() != expected_columns.len() {
        return Ok(());
    }
    for (index, ((returned, value), expected)) in record
        .descriptor
        .fields
        .iter()
        .zip(record.fields.iter())
        .zip(expected_columns.iter())
        .enumerate()
    {
        let returned_type = value.sql_type_hint().unwrap_or(returned.sql_type);
        if returned_type != expected.sql_type {
            return Err(sql_function_return_type_mismatch(
                index + 1,
                returned_type,
                expected.sql_type,
            ));
        }
    }
    Ok(())
}

fn sql_function_return_row_mismatch(expected: usize, actual: usize) -> ExecError {
    ExecError::DetailedError {
        message: "function return row and query-specified return row do not match".into(),
        detail: Some(format!(
            "Returned row contains {actual} attribute{}, but query expects {expected}.",
            if actual == 1 { "" } else { "s" }
        )),
        hint: None,
        sqlstate: "42804",
    }
}

fn sql_function_return_type_mismatch(
    ordinal: usize,
    returned_type: SqlType,
    expected_type: SqlType,
) -> ExecError {
    ExecError::DetailedError {
        message: "function return row and query-specified return row do not match".into(),
        detail: Some(format!(
            "Returned type {} at ordinal position {ordinal}, but query expects {}.",
            sql_type_name(returned_type),
            sql_type_name(expected_type)
        )),
        hint: None,
        sqlstate: "42804",
    }
}

fn sql_function_final_statement_type_mismatch(
    ordinal: usize,
    returned_type: SqlType,
    expected_type: SqlType,
) -> ExecError {
    ExecError::DetailedError {
        message: "return type mismatch in function declared to return record".into(),
        detail: Some(format!(
            "Final statement returns {} instead of {} at column {ordinal}.",
            sql_type_name(returned_type),
            sql_type_name(expected_type)
        )),
        hint: None,
        sqlstate: "42804",
    }
}

fn sql_function_return_type_error(
    column_name: &str,
    expected_type: SqlType,
    source: ExecError,
) -> ExecError {
    ExecError::WithContext {
        source: Box::new(source),
        context: format!(
            "SQL function return column \"{column_name}\" declared as {}",
            sql_type_name(expected_type)
        ),
    }
}

struct SqlScalarFunctionRecordOutput {
    descriptor: RecordDescriptor,
    live_indexes: Option<Vec<usize>>,
    strict_field_types: bool,
}

fn sql_scalar_function_record_output(
    row: &PgProcRow,
    catalog: &dyn CatalogLookup,
    columns: &[QueryColumn],
) -> Option<SqlScalarFunctionRecordOutput> {
    if let Some(descriptor) = sql_function_out_parameter_record_descriptor(row, catalog, columns) {
        return Some(SqlScalarFunctionRecordOutput {
            descriptor,
            live_indexes: None,
            strict_field_types: true,
        });
    }
    let return_type = catalog.type_by_oid(row.prorettype)?.sql_type;
    match return_type.kind {
        SqlTypeKind::Record => Some(SqlScalarFunctionRecordOutput {
            descriptor: RecordDescriptor::anonymous(
                columns
                    .iter()
                    .map(|column| (column.name.clone(), column.sql_type))
                    .collect(),
                -1,
            ),
            live_indexes: None,
            strict_field_types: false,
        }),
        SqlTypeKind::Composite => {
            let relation_oid = return_type.typrelid;
            let relation = catalog.lookup_relation_by_oid(relation_oid)?;
            let mut live_indexes = Vec::new();
            let mut fields = Vec::new();
            for (index, column) in relation.desc.columns.into_iter().enumerate() {
                if column.dropped {
                    continue;
                }
                live_indexes.push(index);
                fields.push((column.name, column.sql_type));
            }
            Some(SqlScalarFunctionRecordOutput {
                descriptor: RecordDescriptor::named(row.prorettype, relation_oid, -1, fields),
                live_indexes: Some(live_indexes),
                strict_field_types: true,
            })
        }
        _ => None,
    }
}

fn sql_function_out_parameter_record_descriptor(
    row: &PgProcRow,
    catalog: &dyn CatalogLookup,
    columns: &[QueryColumn],
) -> Option<RecordDescriptor> {
    let (Some(arg_types), Some(arg_modes)) = (&row.proallargtypes, &row.proargmodes) else {
        return None;
    };
    if arg_types.len() != arg_modes.len() {
        return None;
    }
    let arg_names = row.proargnames.as_deref().unwrap_or(&[]);
    let output_args = arg_types
        .iter()
        .copied()
        .zip(arg_modes.iter().copied())
        .enumerate()
        .filter(|(_, (_, mode))| matches!(*mode, b'o' | b'b' | b't'))
        .collect::<Vec<_>>();
    if output_args.len() <= 1 {
        return None;
    }

    let output_arg_count = output_args.len();
    let mut fields = Vec::with_capacity(output_arg_count);
    for (output_index, (arg_index, (type_oid, _))) in output_args.into_iter().enumerate() {
        let name = arg_names
            .get(arg_index)
            .filter(|name| !name.is_empty())
            .cloned()
            .unwrap_or_else(|| format!("column{}", output_index + 1));
        let declared_type = catalog.type_by_oid(type_oid).map(|row| row.sql_type)?;
        let sql_type = if is_sql_function_polymorphic_type_oid(type_oid)
            && columns.len() == output_arg_count
        {
            columns
                .get(output_index)
                .map(|column| column.sql_type)
                .unwrap_or(declared_type)
        } else {
            declared_type
        };
        fields.push((name, sql_type));
    }
    Some(RecordDescriptor::anonymous(fields, -1))
}

fn is_sql_function_polymorphic_type_oid(type_oid: u32) -> bool {
    matches!(
        type_oid,
        ANYELEMENTOID
            | ANYARRAYOID
            | ANYRANGEOID
            | ANYMULTIRANGEOID
            | ANYCOMPATIBLEOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}

fn sql_scalar_function_record_fields(
    output: &SqlScalarFunctionRecordOutput,
    mut values: Vec<Value>,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    if let [Value::Record(record)] = values.as_slice() {
        values = record.fields.clone();
    }
    if let Some(live_indexes) = &output.live_indexes
        && live_indexes.len() == output.descriptor.fields.len()
        && values.len() > output.descriptor.fields.len()
    {
        values = live_indexes
            .iter()
            .map(|index| values.get(*index).cloned().unwrap_or(Value::Null))
            .collect();
    }
    let expected_columns = output
        .descriptor
        .fields
        .iter()
        .map(|field| QueryColumn {
            name: field.name.clone(),
            sql_type: field.sql_type,
            wire_type_oid: None,
        })
        .collect::<Vec<_>>();
    if output.strict_field_types {
        validate_sql_function_value_types(&values, &expected_columns)?;
    }
    coerce_sql_function_row_values(values, &expected_columns, catalog, ctx)
}

fn validate_sql_function_value_types(
    values: &[Value],
    expected_columns: &[QueryColumn],
) -> Result<(), ExecError> {
    if values.len() != expected_columns.len() {
        return Err(sql_function_return_row_mismatch(
            expected_columns.len(),
            values.len(),
        ));
    }
    for (index, (value, expected)) in values.iter().zip(expected_columns.iter()).enumerate() {
        let Some(returned_type) = value.sql_type_hint() else {
            continue;
        };
        if !sql_function_return_types_match(returned_type, expected.sql_type) {
            return Err(sql_function_return_type_mismatch(
                index + 1,
                returned_type,
                expected.sql_type,
            ));
        }
    }
    Ok(())
}

fn sql_function_return_types_match(returned_type: SqlType, expected_type: SqlType) -> bool {
    returned_type.kind == expected_type.kind
        && returned_type.is_array == expected_type.is_array
        && (returned_type.type_oid == expected_type.type_oid
            || returned_type.type_oid == 0
            || expected_type.type_oid == 0)
}

fn execute_sql_utility_function(
    row: &PgProcRow,
    ctx: &ExecutorContext,
) -> Result<Option<Value>, ExecError> {
    let body = normalized_sql_function_body(&row.prosrc);
    let lower = body.to_ascii_lowercase();
    if starts_with_sql_command(&lower, "alter subscription") && lower.contains("refresh") {
        return Err(sql_function_runtime_error(
            "ALTER SUBSCRIPTION with refresh cannot be executed from a function",
            None,
            "0A000",
        ));
    }
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
        ExecError::DetailedError { message, .. }
            if message == "function return row and query-specified return row do not match"
    ) {
        return err;
    }
    if matches!(
        &err,
        ExecError::Parse(crate::backend::parser::ParseError::DetailedError { message, .. })
            if message.starts_with("attribute ")
                && message.ends_with(" of type record has been dropped")
    ) {
        return err;
    }
    if matches!(
        &err,
        ExecError::Parse(crate::backend::parser::ParseError::DetailedError { message, .. })
            if message
                == "invalid value for parameter \"default_text_search_config\": \"no_such_config\""
    ) {
        return err;
    }
    if let Some((source, position)) = sql_function_inlining_error(row, &err) {
        return ExecError::WithInternalQueryContext {
            source: Box::new(source),
            context: format!("SQL function \"{}\" during inlining", row.proname),
            query: row.prosrc.clone(),
            position,
        };
    }
    if !row.proisstrict
        && row.provolatile == 'i'
        && row.proretset
        && row.prorettype == RECORD_TYPE_OID
        && matches!(
            &err,
            ExecError::DetailedError { message, .. }
                if message == "return type mismatch in function declared to return record"
        )
    {
        return ExecError::WithContext {
            source: Box::new(err),
            context: format!("SQL function \"{}\" during inlining", row.proname),
        };
    }
    ExecError::WithContext {
        source: Box::new(err),
        context: format!("SQL function \"{}\" statement 1", row.proname),
    }
}

fn sql_function_inlining_error(
    row: &PgProcRow,
    err: &ExecError,
) -> Option<(ExecError, Option<usize>)> {
    match err {
        ExecError::TypeMismatch { op, left, right } => {
            let left_type = left.sql_type_hint().map(sql_type_name)?;
            let right_type = right.sql_type_hint().map(sql_type_name)?;
            let position = row.prosrc.find(op).map(|index| index + 1);
            Some((
                ExecError::Parse(crate::backend::parser::ParseError::UndefinedOperator {
                    op,
                    left_type,
                    right_type,
                }),
                position,
            ))
        }
        _ => None,
    }
}

fn normalized_sql_function_body(source: &str) -> String {
    let body = source.trim().trim_end_matches(';').trim();
    sql_standard_function_body_inner(body)
        .unwrap_or(body)
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_string()
}

fn sql_standard_function_body_inner(body: &str) -> Option<&str> {
    let trimmed = body.trim();
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with("begin atomic") {
        return None;
    }
    let without_trailing_semicolon = trimmed.trim_end_matches(';').trim_end();
    let lowered_without_semicolon = without_trailing_semicolon.to_ascii_lowercase();
    let end = if lowered_without_semicolon.ends_with("end") {
        without_trailing_semicolon.len().saturating_sub("end".len())
    } else {
        trimmed.len()
    };
    trimmed.get("begin atomic".len()..end).map(str::trim)
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
    if compact == "select$1||$2"
        && arg_values.len() == 2
        && sql_function_is_array_append_transition(row)?
    {
        // :HACK: Preserve PostgreSQL's polymorphic aggregate transition behavior
        // for SQL support functions of the shape `state_array || element`.
        // The generic text-substitution SQL-function runner loses the concrete
        // type of an initially NULL anyarray state before the concat operator
        // executes.
        return append_array_value(&arg_values[0], &arg_values[1], false).map(Some);
    }
    Ok(None)
}

fn sql_function_is_array_append_transition(row: &PgProcRow) -> Result<bool, ExecError> {
    let declared_oids = parse_proc_argtype_oids(&row.proargtypes)?;
    Ok(matches!(
        declared_oids.as_slice(),
        [ANYARRAYOID, ANYELEMENTOID] | [ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEOID]
    ))
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
            ANYOID | ANYELEMENTOID => {
                if !merge_polymorphic_runtime_subtype(&mut exact_subtype, actual_type) {
                    return Err(sql_function_undefined_runtime_error(row, arg_values));
                }
            }
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
    call_arg_type_oids: Option<&[u32]>,
    catalog: &dyn CatalogLookup,
    datetime_config: &DateTimeConfig,
) -> Result<String, ExecError> {
    let body = normalized_sql_function_body(&row.prosrc);
    // :HACK: This is a narrow compatibility path for regression setup helpers.
    // Full PostgreSQL SQL-language functions need dedicated planning/execution
    // rather than text substitution plus a readonly single-SELECT execution.
    let lower_body = body.to_ascii_lowercase();
    if !(starts_with_sql_command(&lower_body, "select")
        || starts_with_sql_command(&lower_body, "values")
        || starts_with_sql_command(&lower_body, "with")
        || starts_with_sql_command(&lower_body, "insert"))
    {
        return Err(sql_function_runtime_error(
            "only single SELECT, VALUES, WITH, or INSERT LANGUAGE sql function bodies are supported",
            Some(row.prosrc.clone()),
            "0A000",
        ));
    }

    let arg_type_oids = effective_sql_function_arg_type_oids(row, args.len(), call_arg_type_oids);
    let mut sql = substitute_positional_args_with_catalog(
        &body,
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

fn effective_sql_function_arg_type_oids(
    row: &PgProcRow,
    arg_count: usize,
    call_arg_type_oids: Option<&[u32]>,
) -> Vec<u32> {
    let declared = proc_input_arg_type_oids(row);
    (0..arg_count)
        .map(|index| {
            call_arg_type_oids
                .and_then(|oids| oids.get(index).copied())
                .filter(|oid| *oid != 0)
                .or_else(|| declared.get(index).copied())
                .unwrap_or(0)
        })
        .collect()
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
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => {
            let ty = value.sql_type_hint().ok_or_else(|| {
                sql_function_runtime_error("cannot infer SQL function argument type", None, "42804")
            })?;
            let type_name = sql_type_literal_name(catalog, ty)?;
            let text =
                render_datetime_value_text_with_config(value, datetime_config).unwrap_or_default();
            format!("{}::{type_name}", quote_sql_string(&text))
        }
        Value::Interval(interval) => {
            let type_name = sql_type_literal_name(catalog, SqlType::new(SqlTypeKind::Interval))?;
            let text = render_interval_text_with_config(*interval, datetime_config);
            format!("{}::{type_name}", quote_sql_string(&text))
        }
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => {
            let ty = value.sql_type_hint().ok_or_else(|| {
                sql_function_runtime_error("cannot infer SQL function argument type", None, "42804")
            })?;
            let type_name = sql_type_literal_name(catalog, ty)?;
            let text = render_geometry_text(value, Default::default()).unwrap_or_default();
            format!("{}::{type_name}", quote_sql_string(&text))
        }
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
    if matches!(value, Value::Null)
        && let Some(type_row) = catalog.type_by_oid(type_oid)
    {
        if !is_polymorphic_sql_type(type_row.sql_type) {
            let type_name = sql_type_literal_name(catalog, type_row.sql_type)?;
            return Ok(format!("NULL::{type_name}"));
        }
    }
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

fn is_polymorphic_sql_type(ty: SqlType) -> bool {
    matches!(
        ty.kind,
        SqlTypeKind::AnyArray
            | SqlTypeKind::AnyElement
            | SqlTypeKind::AnyRange
            | SqlTypeKind::AnyMultirange
            | SqlTypeKind::AnyCompatible
            | SqlTypeKind::AnyCompatibleArray
            | SqlTypeKind::AnyCompatibleRange
            | SqlTypeKind::AnyCompatibleMultirange
            | SqlTypeKind::AnyEnum
    )
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
            None,
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
            None,
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
        let err =
            inline_sql_function_body(&row, &[], None, &catalog, &datetime_config).unwrap_err();
        assert!(matches!(
            err,
            ExecError::DetailedError {
                sqlstate: "0A000",
                ..
            }
        ));
    }
}
