use std::sync::Arc;

use crate::backend::executor::{
    ExecError, ExecutorContext, Expr, StatementResult, TupleSlot, Value, cast_value, eval_expr,
    eval_plpgsql_expr, execute_planned_stmt,
};
use crate::backend::parser::{CatalogLookup, ParseError, SqlType, SqlTypeKind};
use crate::include::nodes::primnodes::QueryColumn;

use super::ast::RaiseLevel;
use super::compile::{
    CompiledBlock, CompiledExpr, CompiledFunction, CompiledStmt, FunctionReturnContract,
    compile_function_from_proc,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlpgsqlNotice {
    pub level: RaiseLevel,
    pub message: String,
}

#[derive(Debug)]
enum FunctionControl {
    Continue,
    Return,
}

#[derive(Debug)]
struct FunctionState {
    values: Vec<Value>,
    rows: Vec<TupleSlot>,
    scalar_return: Option<Value>,
}

thread_local! {
    static NOTICE_QUEUE: std::cell::RefCell<Vec<PlpgsqlNotice>> = const { std::cell::RefCell::new(Vec::new()) };
}

pub fn take_notices() -> Vec<PlpgsqlNotice> {
    NOTICE_QUEUE.with(|queue| std::mem::take(&mut *queue.borrow_mut()))
}

pub fn clear_notices() {
    NOTICE_QUEUE.with(|queue| queue.borrow_mut().clear());
}

pub(crate) fn execute_block(block: &CompiledBlock) -> Result<StatementResult, ExecError> {
    let mut values = vec![Value::Null; block.total_slots];
    exec_do_block(block, &mut values)?;
    Ok(StatementResult::AffectedRows(0))
}

pub fn execute_user_defined_scalar_function(
    proc_oid: u32,
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let compiled = compiled_function_for_proc(proc_oid, ctx)?;
    let arg_values = args
        .iter()
        .map(|arg| eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;

    let FunctionReturnContract::Scalar { setof, ty, .. } = &compiled.return_contract else {
        return Err(function_runtime_error(
            "record-returning function called in scalar context",
            None,
            "0A000",
        ));
    };
    if *setof {
        return Err(function_runtime_error(
            "set-returning function called in scalar context",
            None,
            "0A000",
        ));
    }

    let mut rows = execute_compiled_function(&compiled, &arg_values, None, ctx)?;
    let mut row = rows.pop().ok_or_else(|| {
        function_runtime_error(
            "control reached end of function without RETURN",
            None,
            "2F005",
        )
    })?;
    let values = row.values()?;
    match values {
        [value] => cast_value(value.clone(), *ty),
        other => Err(function_runtime_error(
            "scalar function returned an unexpected row shape",
            Some(format!("expected 1 column, got {}", other.len())),
            "42804",
        )),
    }
}

pub fn execute_user_defined_set_returning_function(
    proc_oid: u32,
    args: &[Expr],
    output_columns: &[QueryColumn],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let compiled = compiled_function_for_proc(proc_oid, ctx)?;
    let arg_values = args
        .iter()
        .map(|arg| eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    execute_compiled_function(&compiled, &arg_values, Some(output_columns), ctx)
}

fn compiled_function_for_proc(
    proc_oid: u32,
    ctx: &mut ExecutorContext,
) -> Result<Arc<CompiledFunction>, ExecError> {
    if let Some(compiled) = ctx.compiled_functions.get(&proc_oid) {
        return Ok(Arc::clone(compiled));
    }

    let compiled = {
        let catalog = ctx.catalog.as_ref().ok_or_else(|| {
            function_runtime_error(
                "user-defined functions require executor catalog context",
                None,
                "0A000",
            )
        })?;
        let row = catalog.proc_row_by_oid(proc_oid).ok_or_else(|| {
            function_runtime_error(&format!("unknown function oid {proc_oid}"), None, "42883")
        })?;
        if row.prokind != 'f' {
            return Err(function_runtime_error(
                "only functions are executable through the PL/pgSQL runtime",
                Some(format!("prokind = {}", row.prokind)),
                "0A000",
            ));
        }
        let language = catalog.language_row_by_oid(row.prolang).ok_or_else(|| {
            function_runtime_error(
                &format!("unknown language oid {}", row.prolang),
                None,
                "42883",
            )
        })?;
        if !language.lanname.eq_ignore_ascii_case("plpgsql") {
            return Err(function_runtime_error(
                "only LANGUAGE plpgsql functions are supported",
                Some(format!("function language is {}", language.lanname)),
                "0A000",
            ));
        }
        Arc::new(compile_function_from_proc(&row, catalog).map_err(ExecError::Parse)?)
    };

    ctx.compiled_functions
        .insert(proc_oid, Arc::clone(&compiled));
    Ok(compiled)
}

fn execute_compiled_function(
    compiled: &CompiledFunction,
    arg_values: &[Value],
    expected_record_shape: Option<&[QueryColumn]>,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    if compiled.parameter_slots.len() != arg_values.len() {
        return Err(function_runtime_error(
            "function argument count does not match compiled signature",
            Some(format!(
                "expected {}, got {}",
                compiled.parameter_slots.len(),
                arg_values.len()
            )),
            "42883",
        ));
    }

    let mut state = FunctionState {
        values: vec![Value::Null; compiled.body.total_slots],
        rows: Vec::new(),
        scalar_return: None,
    };
    for (slot_def, arg_value) in compiled.parameter_slots.iter().zip(arg_values.iter()) {
        state.values[slot_def.slot] = cast_value(arg_value.clone(), slot_def.ty)?;
    }

    let _ = exec_function_block(
        &compiled.body,
        compiled,
        expected_record_shape,
        &mut state,
        ctx,
    )?;

    match &compiled.return_contract {
        FunctionReturnContract::Scalar {
            ty,
            setof: false,
            output_slot,
        } => {
            if state.scalar_return.is_none() {
                if let Some(slot) = output_slot {
                    state.scalar_return = Some(cast_value(state.values[*slot].clone(), *ty)?);
                } else {
                    return Err(function_runtime_error(
                        "control reached end of function without RETURN",
                        None,
                        "2F005",
                    ));
                }
            }
            Ok(vec![TupleSlot::virtual_row(vec![
                state.scalar_return.expect("scalar return set above"),
            ])])
        }
        FunctionReturnContract::Scalar { setof: true, .. }
        | FunctionReturnContract::FixedRow { .. }
        | FunctionReturnContract::AnonymousRecord { .. } => Ok(state.rows),
    }
}

fn exec_do_block(block: &CompiledBlock, values: &mut [Value]) -> Result<(), ExecError> {
    for local in &block.local_slots {
        values[local.slot] = match &local.default_expr {
            Some(expr) => cast_value(eval_do_expr(expr, values)?, local.ty)?,
            None => Value::Null,
        };
    }
    for stmt in &block.statements {
        exec_do_stmt(stmt, values)?;
    }
    Ok(())
}

fn exec_do_stmt(stmt: &CompiledStmt, values: &mut [Value]) -> Result<(), ExecError> {
    match stmt {
        CompiledStmt::Block(block) => exec_do_block(block, values),
        CompiledStmt::Assign { slot, ty, expr } => {
            values[*slot] = cast_value(eval_do_expr(expr, values)?, *ty)?;
            Ok(())
        }
        CompiledStmt::Null => Ok(()),
        CompiledStmt::If {
            branches,
            else_branch,
        } => {
            for (condition, body) in branches {
                match eval_do_expr(condition, values)? {
                    Value::Bool(true) => {
                        for stmt in body {
                            exec_do_stmt(stmt, values)?;
                        }
                        return Ok(());
                    }
                    Value::Bool(false) | Value::Null => {}
                    other => return Err(ExecError::NonBoolQual(other)),
                }
            }
            for stmt in else_branch {
                exec_do_stmt(stmt, values)?;
            }
            Ok(())
        }
        CompiledStmt::ForInt {
            slot,
            start_expr,
            end_expr,
            body,
        } => {
            let start = match cast_value(
                eval_do_expr(start_expr, values)?,
                SqlType::new(SqlTypeKind::Int4),
            )? {
                Value::Int32(value) => value,
                other => {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "integer FOR start expression",
                        actual: format!("{other:?}"),
                    }));
                }
            };
            let end = match cast_value(
                eval_do_expr(end_expr, values)?,
                SqlType::new(SqlTypeKind::Int4),
            )? {
                Value::Int32(value) => value,
                other => {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "integer FOR end expression",
                        actual: format!("{other:?}"),
                    }));
                }
            };
            if start > end {
                return Ok(());
            }
            for current in start..=end {
                values[*slot] = Value::Int32(current);
                for stmt in body {
                    exec_do_stmt(stmt, values)?;
                }
            }
            Ok(())
        }
        CompiledStmt::Raise {
            level,
            message,
            params,
        } => {
            let param_values = params
                .iter()
                .map(|expr| eval_do_expr(expr, values))
                .collect::<Result<Vec<_>, _>>()?;
            finish_raise(level, message, &param_values)
        }
        CompiledStmt::Return { .. }
        | CompiledStmt::ReturnNext { .. }
        | CompiledStmt::ReturnQuery { .. } => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "RETURN statements are only supported inside CREATE FUNCTION".into(),
            )))
        }
    }
}

fn exec_function_block(
    block: &CompiledBlock,
    compiled: &CompiledFunction,
    expected_record_shape: Option<&[QueryColumn]>,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionControl, ExecError> {
    for local in &block.local_slots {
        state.values[local.slot] = match &local.default_expr {
            Some(expr) => cast_value(eval_function_expr(expr, &state.values, ctx)?, local.ty)?,
            None => Value::Null,
        };
    }
    for stmt in &block.statements {
        if matches!(
            exec_function_stmt(stmt, compiled, expected_record_shape, state, ctx)?,
            FunctionControl::Return
        ) {
            return Ok(FunctionControl::Return);
        }
    }
    Ok(FunctionControl::Continue)
}

fn exec_function_stmt(
    stmt: &CompiledStmt,
    compiled: &CompiledFunction,
    expected_record_shape: Option<&[QueryColumn]>,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionControl, ExecError> {
    match stmt {
        CompiledStmt::Block(block) => {
            exec_function_block(block, compiled, expected_record_shape, state, ctx)
        }
        CompiledStmt::Assign { slot, ty, expr } => {
            state.values[*slot] = cast_value(eval_function_expr(expr, &state.values, ctx)?, *ty)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::Null => Ok(FunctionControl::Continue),
        CompiledStmt::If {
            branches,
            else_branch,
        } => {
            for (condition, body) in branches {
                match eval_function_expr(condition, &state.values, ctx)? {
                    Value::Bool(true) => {
                        return exec_function_stmt_list(
                            body,
                            compiled,
                            expected_record_shape,
                            state,
                            ctx,
                        );
                    }
                    Value::Bool(false) | Value::Null => {}
                    other => return Err(ExecError::NonBoolQual(other)),
                }
            }
            exec_function_stmt_list(else_branch, compiled, expected_record_shape, state, ctx)
        }
        CompiledStmt::ForInt {
            slot,
            start_expr,
            end_expr,
            body,
        } => {
            let start = match cast_value(
                eval_function_expr(start_expr, &state.values, ctx)?,
                SqlType::new(SqlTypeKind::Int4),
            )? {
                Value::Int32(value) => value,
                other => {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "integer FOR start expression",
                        actual: format!("{other:?}"),
                    }));
                }
            };
            let end = match cast_value(
                eval_function_expr(end_expr, &state.values, ctx)?,
                SqlType::new(SqlTypeKind::Int4),
            )? {
                Value::Int32(value) => value,
                other => {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "integer FOR end expression",
                        actual: format!("{other:?}"),
                    }));
                }
            };
            if start > end {
                return Ok(FunctionControl::Continue);
            }
            for current in start..=end {
                state.values[*slot] = Value::Int32(current);
                if matches!(
                    exec_function_stmt_list(body, compiled, expected_record_shape, state, ctx)?,
                    FunctionControl::Return
                ) {
                    return Ok(FunctionControl::Return);
                }
            }
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::Raise {
            level,
            message,
            params,
        } => {
            let param_values = params
                .iter()
                .map(|expr| eval_function_expr(expr, &state.values, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            finish_raise(level, message, &param_values)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::Return { expr } => exec_function_return(expr.as_ref(), compiled, state, ctx),
        CompiledStmt::ReturnNext { expr } => {
            exec_function_return_next(expr.as_ref(), compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::ReturnQuery { plan, .. } => {
            exec_function_return_query(plan, compiled, expected_record_shape, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
    }
}

fn exec_function_stmt_list(
    statements: &[CompiledStmt],
    compiled: &CompiledFunction,
    expected_record_shape: Option<&[QueryColumn]>,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionControl, ExecError> {
    for stmt in statements {
        if matches!(
            exec_function_stmt(stmt, compiled, expected_record_shape, state, ctx)?,
            FunctionControl::Return
        ) {
            return Ok(FunctionControl::Return);
        }
    }
    Ok(FunctionControl::Continue)
}

fn exec_function_return(
    expr: Option<&CompiledExpr>,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionControl, ExecError> {
    match &compiled.return_contract {
        FunctionReturnContract::Scalar {
            ty,
            setof: false,
            output_slot,
        } => {
            state.scalar_return = Some(match expr {
                Some(expr) => cast_value(eval_function_expr(expr, &state.values, ctx)?, *ty)?,
                None => cast_value(
                    state.values[output_slot.ok_or_else(|| {
                        function_runtime_error(
                            "control reached end of function without RETURN",
                            None,
                            "2F005",
                        )
                    })?]
                    .clone(),
                    *ty,
                )?,
            });
            Ok(FunctionControl::Return)
        }
        FunctionReturnContract::Scalar { setof: true, .. }
        | FunctionReturnContract::FixedRow { .. }
        | FunctionReturnContract::AnonymousRecord { .. } => Ok(FunctionControl::Return),
    }
}

fn exec_function_return_next(
    expr: Option<&CompiledExpr>,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    match &compiled.return_contract {
        FunctionReturnContract::Scalar {
            ty, setof: true, ..
        } => {
            let expr = expr.ok_or_else(|| {
                function_runtime_error(
                    "RETURN NEXT requires an expression for scalar set-returning functions",
                    None,
                    "0A000",
                )
            })?;
            let value = cast_value(eval_function_expr(expr, &state.values, ctx)?, *ty)?;
            state.rows.push(TupleSlot::virtual_row(vec![value]));
            Ok(())
        }
        FunctionReturnContract::FixedRow {
            setof: true,
            uses_output_vars: true,
            ..
        } => {
            state.rows.push(current_output_row(compiled, state)?);
            Ok(())
        }
        _ => Err(function_runtime_error(
            "RETURN NEXT is not valid for this function return contract",
            None,
            "0A000",
        )),
    }
}

fn exec_function_return_query(
    plan: &crate::include::nodes::plannodes::PlannedStmt,
    compiled: &CompiledFunction,
    expected_record_shape: Option<&[QueryColumn]>,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    ctx.expr_bindings.outer_tuple = Some(state.values.clone());
    let result = execute_planned_stmt(plan.clone(), ctx);
    ctx.expr_bindings.outer_tuple = None;

    let StatementResult::Query { rows, .. } = result? else {
        return Err(function_runtime_error(
            "RETURN QUERY did not produce rows",
            None,
            "XX000",
        ));
    };

    for row in rows {
        state.rows.push(coerce_function_result_row(
            row,
            &compiled.return_contract,
            expected_record_shape,
        )?);
    }
    Ok(())
}

fn current_output_row(
    compiled: &CompiledFunction,
    state: &FunctionState,
) -> Result<TupleSlot, ExecError> {
    let values = compiled
        .output_slots
        .iter()
        .map(|slot| cast_value(state.values[slot.slot].clone(), slot.column.sql_type))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(TupleSlot::virtual_row(values))
}

fn coerce_function_result_row(
    row: Vec<Value>,
    contract: &FunctionReturnContract,
    expected_record_shape: Option<&[QueryColumn]>,
) -> Result<TupleSlot, ExecError> {
    match contract {
        FunctionReturnContract::Scalar { ty, .. } => match row.as_slice() {
            [value] => Ok(TupleSlot::virtual_row(vec![cast_value(
                value.clone(),
                *ty,
            )?])),
            _ => Err(function_runtime_error(
                "structure of query does not match function result type",
                Some(format!("expected 1 column, got {}", row.len())),
                "42804",
            )),
        },
        FunctionReturnContract::FixedRow { columns, .. } => coerce_row_to_columns(row, columns),
        FunctionReturnContract::AnonymousRecord { .. } => coerce_row_to_columns(
            row,
            expected_record_shape.ok_or_else(|| {
                function_runtime_error(
                    "record-returning function requires a caller-provided row shape",
                    None,
                    "0A000",
                )
            })?,
        ),
    }
}

fn coerce_row_to_columns(row: Vec<Value>, columns: &[QueryColumn]) -> Result<TupleSlot, ExecError> {
    if row.len() != columns.len() {
        return Err(function_runtime_error(
            "structure of query does not match function result type",
            Some(format!(
                "expected {} columns, got {}",
                columns.len(),
                row.len()
            )),
            "42804",
        ));
    }
    let values = row
        .into_iter()
        .zip(columns.iter())
        .map(|(value, column)| cast_value(value, column.sql_type))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(TupleSlot::virtual_row(values))
}

fn eval_do_expr(expr: &CompiledExpr, values: &[Value]) -> Result<Value, ExecError> {
    let mut slot = TupleSlot::virtual_row(values.to_vec());
    eval_plpgsql_expr(&expr.expr, &mut slot)
}

fn eval_function_expr(
    expr: &CompiledExpr,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let mut slot = TupleSlot::virtual_row(values.to_vec());
    eval_expr(&expr.expr, &mut slot, ctx)
}

fn finish_raise(level: &RaiseLevel, message: &str, params: &[Value]) -> Result<(), ExecError> {
    let rendered = render_raise_message(message, params)?;
    match level {
        RaiseLevel::Exception => Err(ExecError::RaiseException(rendered)),
        RaiseLevel::Notice | RaiseLevel::Warning => {
            NOTICE_QUEUE.with(|queue| {
                queue.borrow_mut().push(PlpgsqlNotice {
                    level: level.clone(),
                    message: rendered,
                })
            });
            Ok(())
        }
    }
}

fn render_raise_message(message: &str, params: &[Value]) -> Result<String, ExecError> {
    let mut rendered = String::with_capacity(message.len());
    let mut params = params.iter();
    for ch in message.chars() {
        if ch == '%' {
            let value = params.next().ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "RAISE parameter",
                    actual: message.to_string(),
                })
            })?;
            rendered.push_str(&render_raise_value(value));
        } else {
            rendered.push(ch);
        }
    }
    Ok(rendered)
}

fn render_raise_value(value: &Value) -> String {
    match value {
        Value::Null => "<NULL>".to_string(),
        Value::Text(text) => text.to_string(),
        Value::TextRef(_, _) => value.as_text().unwrap_or_default().to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Money(v) => crate::backend::executor::money_format_text(*v),
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Bit(v) => crate::backend::executor::render_bit_text(v),
        Value::InternalChar(v) => char::from(*v).to_string(),
        Value::Json(text) | Value::JsonPath(text) => text.to_string(),
        Value::Jsonb(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => crate::backend::executor::render_datetime_value_text(value)
            .expect("datetime values render"),
        Value::TsVector(vector) => crate::backend::executor::render_tsvector_text(vector),
        Value::TsQuery(query) => crate::backend::executor::render_tsquery_text(query),
        Value::Bytea(bytes) => {
            let mut rendered = String::from("\\x");
            for byte in bytes {
                use std::fmt::Write as _;
                let _ = write!(&mut rendered, "{byte:02x}");
            }
            rendered
        }
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => {
            crate::backend::executor::render_geometry_text(value, Default::default())
                .unwrap_or_default()
        }
        Value::Range(_) => crate::backend::executor::render_range_text(value).unwrap_or_default(),
        Value::Array(values) => {
            let elems = values.iter().map(render_raise_value).collect::<Vec<_>>();
            format!("{{{}}}", elems.join(","))
        }
        Value::PgArray(array) => crate::backend::executor::value_io::format_array_value_text(array),
        Value::Record(record) => {
            crate::backend::executor::jsonb::jsonb_from_value(&Value::Record(record.clone()))
                .map(|value| value.to_serde().to_string())
                .unwrap_or_default()
        }
    }
}

fn function_runtime_error(
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
