use std::sync::Arc;

use crate::backend::commands::tablecmds::{execute_delete, execute_insert, execute_update};
use crate::backend::executor::{
    ArrayDimension, ArrayValue, ExecError, ExecutorContext, Expr, RelationDesc, StatementResult,
    TupleSlot, Value, cast_value, eval_expr, eval_plpgsql_expr, execute_planned_stmt,
};
use crate::backend::parser::{
    CatalogLookup, ParseError, SqlType, SqlTypeKind, TriggerLevel, TriggerTiming,
};
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::include::catalog::TEXT_TYPE_OID;
use crate::include::nodes::datum::{RecordDescriptor, RecordValue};
use crate::include::nodes::primnodes::QueryColumn;

use super::ast::RaiseLevel;
use super::compile::{
    CompiledBlock, CompiledExpr, CompiledFunction, CompiledSelectIntoTarget, CompiledStmt,
    FunctionReturnContract, TriggerReturnedRow, compile_function_from_proc,
    compile_trigger_function_from_proc,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlpgsqlNotice {
    pub level: RaiseLevel,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct TriggerCallContext {
    pub relation_desc: RelationDesc,
    pub relation_oid: u32,
    pub table_name: String,
    pub table_schema: String,
    pub trigger_name: String,
    pub trigger_args: Vec<String>,
    pub timing: TriggerTiming,
    pub level: TriggerLevel,
    pub op: TriggerOperation,
    pub new_row: Option<Vec<Value>>,
    pub old_row: Option<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerFunctionResult {
    SkipRow,
    ReturnNew(Vec<Value>),
    ReturnOld(Vec<Value>),
    NoValue,
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
    trigger_return: Option<TriggerFunctionResult>,
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

    match &compiled.return_contract {
        FunctionReturnContract::Scalar { setof: true, .. }
        | FunctionReturnContract::FixedRow { setof: true, .. }
        | FunctionReturnContract::AnonymousRecord { setof: true } => {
            return Err(function_runtime_error(
                "set-returning function called in scalar context",
                None,
                "0A000",
            ));
        }
        FunctionReturnContract::Trigger { .. } => {
            return Err(function_runtime_error(
                "trigger function called in scalar context",
                None,
                "0A000",
            ));
        }
        FunctionReturnContract::AnonymousRecord { setof: false } => {
            return Err(function_runtime_error(
                "record-returning function called in scalar context",
                None,
                "0A000",
            ));
        }
        FunctionReturnContract::Scalar { .. } | FunctionReturnContract::FixedRow { .. } => {}
    }

    let track_stats = ctx.session_stats.read().track_functions.tracks_plpgsql();
    if track_stats {
        ctx.session_stats.write().begin_function_call(proc_oid);
    }
    let mut rows = execute_compiled_function(&compiled, &arg_values, None, ctx)?;
    if track_stats {
        ctx.session_stats.write().finish_function_call(proc_oid);
    }
    let mut row = rows.pop().ok_or_else(|| {
        function_runtime_error(
            "control reached end of function without RETURN",
            None,
            "2F005",
        )
    })?;
    let values = row.values()?;
    match &compiled.return_contract {
        FunctionReturnContract::Scalar { ty, .. } => match values {
            [value] => cast_value(value.clone(), *ty),
            other => Err(function_runtime_error(
                "scalar function returned an unexpected row shape",
                Some(format!("expected 1 column, got {}", other.len())),
                "42804",
            )),
        },
        FunctionReturnContract::FixedRow { columns, .. } => {
            let descriptor = anonymous_record_descriptor_for_columns(columns);
            Ok(Value::Record(RecordValue::from_descriptor(
                descriptor,
                values.to_vec(),
            )))
        }
        FunctionReturnContract::AnonymousRecord { .. } | FunctionReturnContract::Trigger { .. } => {
            Err(function_runtime_error(
                "record-returning function called in scalar context",
                None,
                "0A000",
            ))
        }
    }
}

pub fn execute_user_defined_scalar_function_values(
    proc_oid: u32,
    arg_values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let compiled = compiled_function_for_proc(proc_oid, ctx)?;

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

    let track_stats = ctx.session_stats.read().track_functions.tracks_plpgsql();
    if track_stats {
        ctx.session_stats.write().begin_function_call(proc_oid);
    }
    let mut rows = execute_compiled_function(&compiled, arg_values, None, ctx)?;
    if track_stats {
        ctx.session_stats.write().finish_function_call(proc_oid);
    }
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
    let track_stats = ctx.session_stats.read().track_functions.tracks_plpgsql();
    if track_stats {
        ctx.session_stats.write().begin_function_call(proc_oid);
    }
    let result = execute_compiled_function(&compiled, &arg_values, Some(output_columns), ctx);
    if track_stats {
        ctx.session_stats.write().finish_function_call(proc_oid);
    }
    result
}

pub fn execute_user_defined_trigger_function(
    proc_oid: u32,
    call: &TriggerCallContext,
    ctx: &mut ExecutorContext,
) -> Result<TriggerFunctionResult, ExecError> {
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
    let return_type = catalog.type_by_oid(row.prorettype).ok_or_else(|| {
        function_runtime_error(
            &format!("unknown return type oid {}", row.prorettype),
            None,
            "42883",
        )
    })?;
    if return_type.sql_type.kind != SqlTypeKind::Trigger {
        return Err(function_runtime_error(
            "trigger runtime called for a non-trigger function",
            Some(format!("return type is {:?}", return_type.sql_type.kind)),
            "0A000",
        ));
    }
    if row.pronargs != 0 {
        return Err(function_runtime_error(
            "trigger functions must not accept SQL arguments",
            Some(format!("pronargs = {}", row.pronargs)),
            "0A000",
        ));
    }
    let compiled = compile_trigger_function_from_proc(&row, &call.relation_desc, catalog)
        .map_err(ExecError::Parse)?;
    let FunctionReturnContract::Trigger { bindings } = &compiled.return_contract else {
        return Err(function_runtime_error(
            "trigger function compiled with a non-trigger return contract",
            None,
            "0A000",
        ));
    };

    let mut state = FunctionState {
        values: vec![Value::Null; compiled.body.total_slots],
        rows: Vec::new(),
        scalar_return: None,
        trigger_return: None,
    };
    state.values[compiled.found_slot] = Value::Bool(false);
    seed_trigger_state(bindings, call, &mut state);
    let _ = exec_function_block(&compiled.body, &compiled, None, &mut state, ctx)?;
    state.trigger_return.ok_or_else(|| {
        function_runtime_error(
            "control reached end of trigger procedure without RETURN",
            None,
            "2F005",
        )
    })
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
        trigger_return: None,
    };
    state.values[compiled.found_slot] = Value::Bool(false);
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
                if ty.kind == SqlTypeKind::Void {
                    state.scalar_return = Some(Value::Null);
                } else if let Some(slot) = output_slot {
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
        FunctionReturnContract::FixedRow {
            setof: false,
            uses_output_vars: true,
            ..
        } => {
            if state.rows.is_empty() {
                state.rows.push(current_output_row(compiled, &state)?);
            }
            Ok(state.rows)
        }
        FunctionReturnContract::Scalar { setof: true, .. }
        | FunctionReturnContract::FixedRow { .. }
        | FunctionReturnContract::AnonymousRecord { .. } => Ok(state.rows),
        FunctionReturnContract::Trigger { .. } => Err(function_runtime_error(
            "trigger function executed through SQL function path",
            None,
            "0A000",
        )),
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
        CompiledStmt::While { condition, body } => {
            while eval_plpgsql_condition(&eval_do_expr(condition, values)?)? {
                for stmt in body {
                    exec_do_stmt(stmt, values)?;
                }
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
        | CompiledStmt::ReturnTriggerRow { .. }
        | CompiledStmt::ReturnTriggerNull
        | CompiledStmt::ReturnTriggerNoValue
        | CompiledStmt::ReturnQuery { .. }
        | CompiledStmt::Perform { .. }
        | CompiledStmt::SelectInto { .. }
        | CompiledStmt::ExecInsert { .. }
        | CompiledStmt::ExecUpdate { .. }
        | CompiledStmt::ExecDelete { .. } => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "statement is only supported inside CREATE FUNCTION".into(),
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
        CompiledStmt::While { condition, body } => {
            while eval_plpgsql_condition(&eval_function_expr(condition, &state.values, ctx)?)? {
                if matches!(
                    exec_function_stmt_list(body, compiled, expected_record_shape, state, ctx)?,
                    FunctionControl::Return
                ) {
                    return Ok(FunctionControl::Return);
                }
            }
            Ok(FunctionControl::Continue)
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
        CompiledStmt::ReturnTriggerRow { row } => {
            state.trigger_return = Some(current_trigger_return(compiled, state, *row)?);
            Ok(FunctionControl::Return)
        }
        CompiledStmt::ReturnTriggerNull => {
            state.trigger_return = Some(TriggerFunctionResult::SkipRow);
            Ok(FunctionControl::Return)
        }
        CompiledStmt::ReturnTriggerNoValue => {
            state.trigger_return = Some(TriggerFunctionResult::NoValue);
            Ok(FunctionControl::Return)
        }
        CompiledStmt::ReturnQuery { plan, .. } => {
            exec_function_return_query(plan, compiled, expected_record_shape, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::Perform { plan } => {
            exec_function_perform(plan, compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::SelectInto { plan, targets } => {
            exec_function_select_into(plan, targets, compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::ExecInsert { stmt } => {
            exec_function_insert(stmt, compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::ExecUpdate { stmt } => {
            exec_function_update(stmt, compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::ExecDelete { stmt } => {
            exec_function_delete(stmt, compiled, state, ctx)?;
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
        FunctionReturnContract::Trigger { .. } => Err(function_runtime_error(
            "trigger functions must return NEW, OLD, or NULL",
            None,
            "0A000",
        )),
        FunctionReturnContract::Scalar {
            ty,
            setof: false,
            output_slot,
        } => {
            state.scalar_return = Some(match expr {
                Some(expr) => cast_value(eval_function_expr(expr, &state.values, ctx)?, *ty)?,
                None if ty.kind == SqlTypeKind::Void => Value::Null,
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
        FunctionReturnContract::Trigger { .. } => Err(function_runtime_error(
            "RETURN NEXT is not valid for trigger functions",
            None,
            "0A000",
        )),
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

fn exec_function_perform(
    plan: &crate::include::nodes::plannodes::PlannedStmt,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let rows = execute_function_query_rows(plan, compiled, state, ctx)?;
    state.values[compiled.found_slot] = Value::Bool(!rows.is_empty());
    Ok(())
}

fn exec_function_select_into(
    plan: &crate::include::nodes::plannodes::PlannedStmt,
    targets: &[CompiledSelectIntoTarget],
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let rows = execute_function_query_rows(plan, compiled, state, ctx)?;
    let Some(row) = rows.first() else {
        for target in targets {
            state.values[target.slot] = Value::Null;
        }
        state.values[compiled.found_slot] = Value::Bool(false);
        return Ok(());
    };

    match targets {
        [CompiledSelectIntoTarget { slot, ty }]
            if matches!(ty.kind, SqlTypeKind::Record | SqlTypeKind::Composite) =>
        {
            state.values[*slot] = Value::Record(RecordValue::from_descriptor(
                anonymous_record_descriptor_for_columns(&plan.columns()),
                row.clone(),
            ));
        }
        [CompiledSelectIntoTarget { slot, ty }] => {
            let value = row.first().cloned().unwrap_or(Value::Null);
            state.values[*slot] = cast_value(value, *ty)?;
        }
        _ => {
            if row.len() != targets.len() {
                return Err(function_runtime_error(
                    "query returned an unexpected row shape",
                    Some(format!(
                        "expected {} columns, got {}",
                        targets.len(),
                        row.len()
                    )),
                    "42804",
                ));
            }
            for (target, value) in targets.iter().zip(row.iter()) {
                state.values[target.slot] = cast_value(value.clone(), target.ty)?;
            }
        }
    }
    state.values[compiled.found_slot] = Value::Bool(true);
    Ok(())
}

fn exec_function_insert(
    stmt: &crate::backend::parser::BoundInsertStatement,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let catalog = ctx.catalog.clone().ok_or_else(|| {
        function_runtime_error(
            "user-defined functions require executor catalog context",
            None,
            "0A000",
        )
    })?;
    ctx.expr_bindings.outer_tuple = Some(function_outer_tuple(compiled, state));
    let xid = ctx.ensure_write_xid()?;
    let result = execute_insert(stmt.clone(), &catalog, ctx, xid, ctx.next_command_id);
    ctx.expr_bindings.outer_tuple = None;
    let result = result?;
    state.values[compiled.found_slot] = Value::Bool(statement_result_changed_rows(&result));
    Ok(())
}

fn exec_function_update(
    stmt: &crate::backend::parser::BoundUpdateStatement,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let catalog = ctx.catalog.clone().ok_or_else(|| {
        function_runtime_error(
            "user-defined functions require executor catalog context",
            None,
            "0A000",
        )
    })?;
    ctx.expr_bindings.outer_tuple = Some(function_outer_tuple(compiled, state));
    let xid = ctx.ensure_write_xid()?;
    let result = execute_update(stmt.clone(), &catalog, ctx, xid, ctx.next_command_id);
    ctx.expr_bindings.outer_tuple = None;
    let result = result?;
    state.values[compiled.found_slot] = Value::Bool(statement_result_changed_rows(&result));
    Ok(())
}

fn exec_function_delete(
    stmt: &crate::backend::parser::BoundDeleteStatement,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let catalog = ctx.catalog.clone().ok_or_else(|| {
        function_runtime_error(
            "user-defined functions require executor catalog context",
            None,
            "0A000",
        )
    })?;
    ctx.expr_bindings.outer_tuple = Some(function_outer_tuple(compiled, state));
    let xid = ctx.ensure_write_xid()?;
    let result = execute_delete(stmt.clone(), &catalog, ctx, xid);
    ctx.expr_bindings.outer_tuple = None;
    let result = result?;
    state.values[compiled.found_slot] = Value::Bool(statement_result_changed_rows(&result));
    Ok(())
}

fn execute_function_query_rows(
    plan: &crate::include::nodes::plannodes::PlannedStmt,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Vec<Value>>, ExecError> {
    ctx.expr_bindings.outer_tuple = Some(function_outer_tuple(compiled, state));
    let result = execute_planned_stmt(plan.clone(), ctx);
    ctx.expr_bindings.outer_tuple = None;
    let StatementResult::Query { rows, .. } = result? else {
        return Err(function_runtime_error(
            "PL/pgSQL SQL statement did not produce rows",
            None,
            "XX000",
        ));
    };
    Ok(rows)
}

fn function_outer_tuple(compiled: &CompiledFunction, state: &FunctionState) -> Vec<Value> {
    let mut values = state.values.clone();
    if let FunctionReturnContract::Trigger { bindings } = &compiled.return_contract {
        values.push(trigger_relation_record_value(&bindings.new_row, state));
        values.push(trigger_relation_record_value(&bindings.old_row, state));
    }
    values
}

fn trigger_relation_record_value(
    relation: &super::compile::CompiledTriggerRelation,
    state: &FunctionState,
) -> Value {
    Value::Record(RecordValue::anonymous(
        relation
            .slots
            .iter()
            .zip(relation.field_names.iter())
            .map(|(slot, name)| (name.clone(), state.values[*slot].clone()))
            .collect(),
    ))
}

fn statement_result_changed_rows(result: &StatementResult) -> bool {
    match result {
        StatementResult::AffectedRows(rows) => *rows > 0,
        StatementResult::Query { rows, .. } => !rows.is_empty(),
    }
}

fn anonymous_record_descriptor_for_columns(columns: &[QueryColumn]) -> RecordDescriptor {
    assign_anonymous_record_descriptor(
        columns
            .iter()
            .map(|column| (column.name.clone(), column.sql_type))
            .collect(),
    )
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
        FunctionReturnContract::Trigger { .. } => Err(function_runtime_error(
            "trigger functions do not produce SQL rows",
            None,
            "0A000",
        )),
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

fn eval_plpgsql_condition(value: &Value) -> Result<bool, ExecError> {
    match value {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) | Value::Null => Ok(false),
        other => Err(ExecError::NonBoolQual(other.clone())),
    }
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
        Value::Xml(text) => text.to_string(),
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
        Value::Multirange(_) => {
            crate::backend::executor::render_multirange_text(value).unwrap_or_default()
        }
        Value::Array(values) => {
            let elems = values.iter().map(render_raise_value).collect::<Vec<_>>();
            format!("{{{}}}", elems.join(","))
        }
        Value::PgArray(array) => crate::backend::executor::value_io::format_array_value_text(array),
        Value::Record(record) => crate::backend::executor::jsonb::jsonb_from_value(
            &Value::Record(record.clone()),
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        )
        .map(|value| value.to_serde().to_string())
        .unwrap_or_default(),
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

fn seed_trigger_state(
    bindings: &super::compile::CompiledTriggerBindings,
    call: &TriggerCallContext,
    state: &mut FunctionState,
) {
    seed_trigger_relation(&bindings.new_row, call.new_row.as_ref(), state);
    seed_trigger_relation(&bindings.old_row, call.old_row.as_ref(), state);
    state.values[bindings.tg_name_slot] = Value::Text(call.trigger_name.clone().into());
    state.values[bindings.tg_op_slot] = Value::Text(
        match call.op {
            TriggerOperation::Insert => "INSERT",
            TriggerOperation::Update => "UPDATE",
            TriggerOperation::Delete => "DELETE",
        }
        .into(),
    );
    state.values[bindings.tg_when_slot] = Value::Text(
        match call.timing {
            TriggerTiming::Before => "BEFORE",
            TriggerTiming::After => "AFTER",
            TriggerTiming::InsteadOf => "INSTEAD OF",
        }
        .into(),
    );
    state.values[bindings.tg_level_slot] = Value::Text(
        match call.level {
            TriggerLevel::Row => "ROW",
            TriggerLevel::Statement => "STATEMENT",
        }
        .into(),
    );
    state.values[bindings.tg_relid_slot] = Value::Int32(call.relation_oid as i32);
    state.values[bindings.tg_nargs_slot] = Value::Int32(call.trigger_args.len() as i32);
    let tg_argv = if call.trigger_args.is_empty() {
        ArrayValue::empty()
    } else {
        ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound: 0,
                length: call.trigger_args.len(),
            }],
            call.trigger_args
                .iter()
                .cloned()
                .map(|arg| Value::Text(arg.into()))
                .collect(),
        )
    };
    state.values[bindings.tg_argv_slot] =
        Value::PgArray(tg_argv.with_element_type_oid(TEXT_TYPE_OID));
    state.values[bindings.tg_table_name_slot] = Value::Text(call.table_name.clone().into());
    state.values[bindings.tg_table_schema_slot] = Value::Text(call.table_schema.clone().into());
}

fn seed_trigger_relation(
    relation: &super::compile::CompiledTriggerRelation,
    source: Option<&Vec<Value>>,
    state: &mut FunctionState,
) {
    let Some(source) = source else {
        return;
    };
    for (slot, value) in relation.slots.iter().copied().zip(source.iter()) {
        state.values[slot] = value.clone();
    }
}

fn current_trigger_return(
    compiled: &CompiledFunction,
    state: &FunctionState,
    returned_row: TriggerReturnedRow,
) -> Result<TriggerFunctionResult, ExecError> {
    let FunctionReturnContract::Trigger { bindings } = &compiled.return_contract else {
        return Err(function_runtime_error(
            "trigger return reached a non-trigger function",
            None,
            "0A000",
        ));
    };
    let relation = match returned_row {
        TriggerReturnedRow::New => &bindings.new_row,
        TriggerReturnedRow::Old => &bindings.old_row,
    };
    let values = relation
        .slots
        .iter()
        .map(|slot| state.values[*slot].clone())
        .collect::<Vec<_>>();
    Ok(match returned_row {
        TriggerReturnedRow::New => TriggerFunctionResult::ReturnNew(values),
        TriggerReturnedRow::Old => TriggerFunctionResult::ReturnOld(values),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::{TriggerLevel, TriggerTiming};
    use crate::pl::plpgsql::compile::{CompiledTriggerBindings, CompiledTriggerRelation};

    #[test]
    fn seed_trigger_state_uses_zero_based_tg_argv() {
        let bindings = CompiledTriggerBindings {
            new_row: CompiledTriggerRelation {
                slots: vec![],
                field_names: vec![],
            },
            old_row: CompiledTriggerRelation {
                slots: vec![],
                field_names: vec![],
            },
            tg_name_slot: 0,
            tg_op_slot: 1,
            tg_when_slot: 2,
            tg_level_slot: 3,
            tg_relid_slot: 4,
            tg_nargs_slot: 5,
            tg_argv_slot: 6,
            tg_table_name_slot: 7,
            tg_table_schema_slot: 8,
        };
        let call = TriggerCallContext {
            relation_desc: RelationDesc { columns: vec![] },
            relation_oid: 42,
            table_name: "main_table".into(),
            table_schema: "public".into(),
            trigger_name: "before_ins_stmt".into(),
            trigger_args: vec!["before_ins_stmt".into()],
            timing: TriggerTiming::Before,
            level: TriggerLevel::Statement,
            op: TriggerOperation::Insert,
            new_row: None,
            old_row: None,
        };
        let mut state = FunctionState {
            values: vec![Value::Null; 9],
            rows: Vec::new(),
            scalar_return: None,
            trigger_return: None,
        };

        seed_trigger_state(&bindings, &call, &mut state);

        assert_eq!(state.values[bindings.tg_nargs_slot], Value::Int32(1));
        assert_eq!(
            state.values[bindings.tg_argv_slot],
            Value::PgArray(
                ArrayValue::from_dimensions(
                    vec![ArrayDimension {
                        lower_bound: 0,
                        length: 1,
                    }],
                    vec![Value::Text("before_ins_stmt".into())],
                )
                .with_element_type_oid(TEXT_TYPE_OID),
            )
        );
    }

    #[test]
    fn seed_trigger_state_uses_empty_array_for_no_trigger_args() {
        let bindings = CompiledTriggerBindings {
            new_row: CompiledTriggerRelation {
                slots: vec![],
                field_names: vec![],
            },
            old_row: CompiledTriggerRelation {
                slots: vec![],
                field_names: vec![],
            },
            tg_name_slot: 0,
            tg_op_slot: 1,
            tg_when_slot: 2,
            tg_level_slot: 3,
            tg_relid_slot: 4,
            tg_nargs_slot: 5,
            tg_argv_slot: 6,
            tg_table_name_slot: 7,
            tg_table_schema_slot: 8,
        };
        let call = TriggerCallContext {
            relation_desc: RelationDesc { columns: vec![] },
            relation_oid: 42,
            table_name: "main_table".into(),
            table_schema: "public".into(),
            trigger_name: "after_ins_stmt".into(),
            trigger_args: vec![],
            timing: TriggerTiming::After,
            level: TriggerLevel::Statement,
            op: TriggerOperation::Insert,
            new_row: None,
            old_row: None,
        };
        let mut state = FunctionState {
            values: vec![Value::Null; 9],
            rows: Vec::new(),
            scalar_return: None,
            trigger_return: None,
        };

        seed_trigger_state(&bindings, &call, &mut state);

        assert_eq!(
            state.values[bindings.tg_argv_slot],
            Value::PgArray(ArrayValue::empty().with_element_type_oid(TEXT_TYPE_OID))
        );
    }
}
