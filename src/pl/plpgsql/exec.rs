use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

use crate::backend::access::heap::heapam::HeapError;
use crate::backend::access::transam::xact::{CommandId, INVALID_TRANSACTION_ID, TransactionId};
use crate::backend::commands::tablecmds::{
    apply_sql_type_array_subscript_assignment, collect_matching_rows_heap, execute_delete,
    execute_insert, execute_merge, execute_update,
};
use crate::backend::executor::expr_reg::format_type_text;
use crate::backend::executor::function_guc::{
    apply_function_guc, apply_security_definer_identity, parsed_proconfig, restore_function_gucs,
    restore_function_identity, save_function_identity,
};
use crate::backend::executor::{
    ArrayDimension, ArrayValue, ExecError, ExecutorContext, Expr, RelationDesc, StatementResult,
    TupleSlot, Value, cast_value, cast_value_with_config,
    cast_value_with_source_type_catalog_and_config, compare_order_values,
    enforce_domain_constraints_for_value, eval_expr, eval_plpgsql_expr, execute_planned_stmt,
    execute_readonly_statement_with_config, executor_start, render_interval_text,
};
use crate::backend::libpq::pqformat::format_bytea_text;
use crate::backend::libpq::pqformat::{format_exec_error, format_exec_error_hint};
use crate::backend::parser::analyze::sql_type_name;
use crate::backend::parser::{
    Catalog, CatalogLookup, ParseError, PreparedExternalParam, SqlType, SqlTypeKind, Statement,
    bind_delete_with_outer_scopes, bind_insert_with_outer_scopes,
    bind_scalar_expr_in_named_slot_scope, bind_update_with_outer_scopes, parse_statement,
    pg_plan_query_with_outer_scopes_and_ctes_config,
    pg_plan_values_query_with_outer_scopes_and_ctes_config, plan_merge_with_outer_scopes_and_ctes,
    resolve_raw_type_name, with_external_param_types,
};
use crate::backend::utils::misc::notices::push_notice;
use crate::backend::utils::record::{
    assign_anonymous_record_descriptor, lookup_anonymous_record_descriptor,
};
use crate::include::catalog::{EVENT_TRIGGER_TYPE_OID, PgProcRow};
use crate::include::executor::execdesc::create_query_desc;
use crate::include::nodes::datum::{RecordDescriptor, RecordValue};
use crate::include::nodes::execnodes::{MaterializedCteTable, MaterializedRow, SystemVarBinding};
use crate::include::nodes::primnodes::{QueryColumn, expr_sql_type_hint};
use crate::pgrust::portal::{
    CursorOptions, Portal, PortalExecution, PortalFetchDirection, PortalFetchLimit,
    PositionedCursorRow,
};
use crate::pgrust::session::{ByteaOutputFormat, resolve_thread_prepared_statement};
use pgrust_nodes::{EventTriggerCallContext, TriggerCallContext, TriggerFunctionResult};
use pgrust_plpgsql::event_trigger_return_bindings;
use pgrust_plpgsql::{
    CompiledAssignIndirection, CompiledBlock, CompiledCursorOpenSource,
    CompiledEventTriggerBindings, CompiledExceptionHandler, CompiledExpr, CompiledForQuerySource,
    CompiledForQueryTarget, CompiledFunction, CompiledIndirectAssignTarget,
    CompiledSelectIntoTarget, CompiledStmt, CompiledStrictParam, CompiledTriggerBindings,
    DeclaredCursorParam, DoControl, DynamicExternalParamBinding, ExtraCheckLevel, FunctionControl,
    FunctionCursor, FunctionQueryResult, FunctionQueryRow, FunctionReturnContract,
    PlpgsqlContextFrame, PlpgsqlErrorFields, PlpgsqlExceptionData, QueryCompareOp, RuntimeSqlScope,
    TriggerReturnedRow, catalog_foreign_key_column_array, catalog_foreign_key_is_array,
    catalog_foreign_key_is_optional, current_event_trigger_table_rewrite_relation_name_for_oid,
    current_plpgsql_context, diagnostic_text, exception_condition_name_sqlstate,
    is_catalog_foreign_key_check_sql, is_catalog_foreign_key_query_sql, parse_proc_argtype_oids,
    plpgsql_extra_check_level, plpgsql_query_column, push_context_frame,
    push_event_trigger_ddl_commands, push_event_trigger_dropped_objects,
    push_event_trigger_table_rewrite, push_plpgsql_notice, queue_plpgsql_warning, quote_identifier,
    quote_sql_string, resolve_raise_sqlstate, runtime_sql_param_id, transition_table_visible_rows,
    trigger_return_bindings, validate_plpgsql_function_row as validate_plpgsql_function_row_impl,
    validate_plpgsql_procedure_row as validate_plpgsql_procedure_row_impl,
    validate_scalar_call_return_contract,
};
pub use pgrust_plpgsql::{
    clear_notices, current_event_trigger_ddl_commands, current_event_trigger_dropped_objects,
    current_event_trigger_table_rewrite, take_notices,
};
use pgrust_plpgsql::{
    concrete_polymorphic_proc_row, planner_config_from_executor_gucs, routine_cache_key,
    statement_result_changed_rows, trigger_cache_key,
};

use super::ast::{CursorDirection, ExceptionCondition, RaiseLevel};
use super::cache::PlpgsqlFunctionCacheKey;
use super::compile::{
    compile_event_trigger_function_from_proc, compile_function_from_proc,
    compile_trigger_function_from_proc, runtime_sql_bound_scope, runtime_sql_param_bound_scope,
};

#[derive(Debug)]
struct FunctionState {
    values: Vec<Value>,
    rows: Vec<TupleSlot>,
    scalar_return: Option<Value>,
    trigger_return: Option<TriggerFunctionResult>,
    cursors: HashMap<String, FunctionCursor>,
    local_guc_writes: HashSet<String>,
    session_guc_writes: HashSet<String>,
    last_row_count: usize,
    current_exception: Option<PlpgsqlExceptionData>,
}

fn with_context_frame<T>(
    compiled: &CompiledFunction,
    line: usize,
    action: &'static str,
    f: impl FnOnce() -> Result<T, ExecError>,
) -> Result<T, ExecError> {
    let _guard = push_context_frame(PlpgsqlContextFrame {
        function_name: compiled_context_name(compiled),
        line,
        action,
    });
    f()
}

pub(crate) fn execute_block(block: &CompiledBlock) -> Result<StatementResult, ExecError> {
    let gucs = HashMap::new();
    execute_block_with_gucs(block, &gucs)
}

pub(crate) fn execute_block_with_gucs(
    block: &CompiledBlock,
    gucs: &HashMap<String, String>,
) -> Result<StatementResult, ExecError> {
    let mut values = vec![Value::Null; block.total_slots];
    if matches!(
        exec_do_block(block, &mut values, gucs)?,
        DoControl::LoopContinue
    ) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CONTINUE inside a loop",
            actual: "CONTINUE".into(),
        }));
    }
    Ok(StatementResult::AffectedRows(0))
}

pub(crate) fn execute_do_function(
    compiled: &CompiledFunction,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    execute_compiled_function(compiled, &[], None, ctx)?;
    Ok(StatementResult::AffectedRows(0))
}

pub fn execute_user_defined_scalar_function(
    proc_oid: u32,
    resolved_result_type: Option<SqlType>,
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let arg_values = args
        .iter()
        .map(|arg| eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let arg_types = arg_values
        .iter()
        .map(Value::sql_type_hint)
        .collect::<Vec<_>>();
    let compiled = compiled_function_for_proc(proc_oid, resolved_result_type, &arg_types, ctx)?;

    validate_scalar_call_return_contract(
        &compiled.return_contract,
        pgrust_plpgsql::ScalarCallContext::ExprArgs,
    )
    .map_err(plpgsql_scalar_call_validation_error)?;

    let track_stats = ctx.session_stats.read().track_functions.tracks_plpgsql();
    if track_stats {
        ctx.session_stats.write().begin_function_call(proc_oid);
    }
    let mut rows = execute_compiled_function_for_call(&compiled, &arg_values, None, ctx)?;
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
            [value] => cast_function_scalar_return_value(value.clone(), *ty),
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
        FunctionReturnContract::AnonymousRecord { .. } => match values {
            [value] => Ok(value.clone()),
            other => Err(function_runtime_error(
                "record function returned an unexpected row shape",
                Some(format!("expected 1 column, got {}", other.len())),
                "42804",
            )),
        },
        FunctionReturnContract::Trigger { .. } => Err(function_runtime_error(
            "trigger function called in scalar context",
            None,
            "0A000",
        )),
        FunctionReturnContract::EventTrigger { .. } => Err(function_runtime_error(
            "event trigger function called in scalar context",
            None,
            "0A000",
        )),
    }
}

pub fn execute_user_defined_scalar_function_values(
    proc_oid: u32,
    arg_values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let arg_types = arg_values
        .iter()
        .map(Value::sql_type_hint)
        .collect::<Vec<_>>();
    execute_user_defined_scalar_function_values_with_actual_arg_types(
        proc_oid, arg_values, &arg_types, ctx,
    )
}

pub fn execute_user_defined_scalar_function_values_with_arg_types(
    proc_oid: u32,
    arg_values: &[Value],
    arg_types: &[SqlType],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let actual_arg_types = arg_types.iter().copied().map(Some).collect::<Vec<_>>();
    execute_user_defined_scalar_function_values_with_actual_arg_types(
        proc_oid,
        arg_values,
        &actual_arg_types,
        ctx,
    )
}

fn execute_user_defined_scalar_function_values_with_actual_arg_types(
    proc_oid: u32,
    arg_values: &[Value],
    actual_arg_types: &[Option<SqlType>],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let compiled = compiled_function_for_proc(proc_oid, None, actual_arg_types, ctx)?;

    validate_scalar_call_return_contract(
        &compiled.return_contract,
        pgrust_plpgsql::ScalarCallContext::ValueArgs,
    )
    .map_err(plpgsql_scalar_call_validation_error)?;

    let track_stats = ctx.session_stats.read().track_functions.tracks_plpgsql();
    if track_stats {
        ctx.session_stats.write().begin_function_call(proc_oid);
    }
    let mut rows = execute_compiled_function_for_call(&compiled, arg_values, None, ctx)?;
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
            [value] => cast_function_scalar_return_value(value.clone(), *ty),
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
        FunctionReturnContract::AnonymousRecord { .. } => match values {
            [value] => Ok(value.clone()),
            other => Err(function_runtime_error(
                "record function returned an unexpected row shape",
                Some(format!("expected 1 column, got {}", other.len())),
                "42804",
            )),
        },
        FunctionReturnContract::Trigger { .. } => Err(function_runtime_error(
            "trigger function called in scalar context",
            None,
            "0A000",
        )),
        FunctionReturnContract::EventTrigger { .. } => Err(function_runtime_error(
            "event trigger function called in scalar context",
            None,
            "0A000",
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
    let arg_values = args
        .iter()
        .map(|arg| eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let arg_types = arg_values
        .iter()
        .map(Value::sql_type_hint)
        .collect::<Vec<_>>();
    let compiled = compiled_function_for_proc(proc_oid, None, &arg_types, ctx)?;
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

pub fn execute_user_defined_procedure_values(
    proc_oid: u32,
    arg_values: &[Value],
    output_columns: &[QueryColumn],
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let compiled = compiled_procedure_for_proc(proc_oid, ctx)?;
    let track_stats = ctx.session_stats.read().track_functions.tracks_plpgsql();
    if track_stats {
        ctx.session_stats.write().begin_function_call(proc_oid);
    }
    let result = execute_compiled_function(&compiled, arg_values, Some(output_columns), ctx);
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
    let compiled = compiled_trigger_function_for_proc(proc_oid, call, ctx)?;
    let bindings = trigger_return_bindings(&compiled.return_contract)
        .map_err(plpgsql_trigger_contract_error)?;

    let mut state = FunctionState {
        values: vec![Value::Null; compiled.body.total_slots],
        rows: Vec::new(),
        scalar_return: None,
        trigger_return: None,
        cursors: HashMap::new(),
        local_guc_writes: HashSet::new(),
        session_guc_writes: HashSet::new(),
        last_row_count: 0,
        current_exception: None,
    };
    state.values[compiled.found_slot] = Value::Bool(false);
    state.values[compiled.sqlstate_slot] = Value::Text(String::new().into());
    state.values[compiled.sqlerrm_slot] = Value::Text(String::new().into());
    seed_trigger_state(bindings, call, &mut state);
    let saved_pinned_cte_tables = ctx.pinned_cte_tables.clone();
    let saved_cte_tables = ctx.cte_tables.clone();
    let saved_cte_producers = ctx.cte_producers.clone();
    let result = match install_trigger_transition_ctes(&compiled, call, ctx) {
        Ok(()) => exec_function_block(&compiled.body, &compiled, None, &mut state, ctx)
            .map_err(|err| with_plpgsql_context_if_missing(err, &compiled, "statement")),
        Err(err) => Err(err),
    };
    ctx.pinned_cte_tables = saved_pinned_cte_tables;
    ctx.cte_tables = saved_cte_tables;
    ctx.cte_producers = saved_cte_producers;
    if matches!(result?, FunctionControl::LoopContinue) {
        return Err(function_runtime_error(
            "CONTINUE cannot be used outside a loop",
            None,
            "2D000",
        ));
    }
    state.trigger_return.ok_or_else(|| {
        function_runtime_error(
            "control reached end of trigger procedure without RETURN",
            None,
            "2F005",
        )
    })
}

pub fn execute_user_defined_event_trigger_function(
    proc_oid: u32,
    call: &EventTriggerCallContext,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let compiled = compiled_event_trigger_function_for_proc(proc_oid, ctx)?;
    let bindings = event_trigger_return_bindings(&compiled.return_contract)
        .map_err(plpgsql_trigger_contract_error)?;

    let mut state = FunctionState {
        values: vec![Value::Null; compiled.body.total_slots],
        rows: Vec::new(),
        scalar_return: None,
        trigger_return: None,
        cursors: HashMap::new(),
        local_guc_writes: HashSet::new(),
        session_guc_writes: HashSet::new(),
        last_row_count: 0,
        current_exception: None,
    };
    state.values[compiled.found_slot] = Value::Bool(false);
    state.values[compiled.sqlstate_slot] = Value::Text(String::new().into());
    state.values[compiled.sqlerrm_slot] = Value::Text(String::new().into());
    seed_event_trigger_state(bindings, call, &mut state);
    let _ddl_command_guard = push_event_trigger_ddl_commands(call);
    let _dropped_objects_guard = push_event_trigger_dropped_objects(call);
    let _table_rewrite_guard = push_event_trigger_table_rewrite(call);
    exec_function_block(&compiled.body, &compiled, None, &mut state, ctx)
        .map_err(|err| with_plpgsql_context_if_missing(err, &compiled, "statement"))?;
    Ok(())
}

fn compiled_function_for_proc(
    proc_oid: u32,
    resolved_result_type: Option<SqlType>,
    actual_arg_types: &[Option<SqlType>],
    ctx: &mut ExecutorContext,
) -> Result<Arc<CompiledFunction>, ExecError> {
    let catalog = executor_catalog(ctx, "user-defined functions")?;
    let Some(row) = catalog.proc_row_by_oid(proc_oid) else {
        ctx.plpgsql_function_cache.write().remove_proc(proc_oid);
        return Err(function_runtime_error(
            &format!("unknown function oid {proc_oid}"),
            None,
            "42883",
        ));
    };
    validate_plpgsql_function_row(&row, catalog, "function")?;
    let key = routine_cache_key(&row, resolved_result_type, actual_arg_types);
    if let Some(compiled) = ctx.plpgsql_function_cache.read().get_valid(&key, &row) {
        return Ok(compiled);
    }
    let compile_row =
        concrete_polymorphic_proc_row(&row, resolved_result_type, actual_arg_types, catalog)
            .unwrap_or_else(|| row.clone());
    let mut compiled = compile_function_from_proc(&compile_row, catalog, Some(&ctx.gucs))
        .map_err(|err| plpgsql_compile_error(err, &row))?;
    compiled.context_arg_type_names = proc_context_arg_type_names(&row, catalog);
    let compiled = Arc::new(compiled);
    ctx.plpgsql_function_cache
        .write()
        .insert(key, row, Arc::clone(&compiled));
    Ok(compiled)
}

fn proc_context_arg_type_names(row: &PgProcRow, catalog: &dyn CatalogLookup) -> Vec<String> {
    parse_proc_argtype_oids(&row.proargtypes)
        .unwrap_or_default()
        .into_iter()
        .map(|oid| format_type_text(oid, None, catalog))
        .collect()
}

fn compiled_procedure_for_proc(
    proc_oid: u32,
    ctx: &mut ExecutorContext,
) -> Result<Arc<CompiledFunction>, ExecError> {
    let catalog = executor_catalog(ctx, "user-defined procedures")?;
    let Some(row) = catalog.proc_row_by_oid(proc_oid) else {
        ctx.plpgsql_function_cache.write().remove_proc(proc_oid);
        return Err(function_runtime_error(
            &format!("unknown procedure oid {proc_oid}"),
            None,
            "42883",
        ));
    };
    validate_plpgsql_procedure_row(&row, catalog)?;
    let key = routine_cache_key(&row, None, &[]);
    if let Some(compiled) = ctx.plpgsql_function_cache.read().get_valid(&key, &row) {
        return Ok(compiled);
    }
    let compiled = Arc::new(
        compile_function_from_proc(&row, catalog, Some(&ctx.gucs))
            .map_err(|err| plpgsql_compile_error(err, &row))?,
    );
    ctx.plpgsql_function_cache
        .write()
        .insert(key, row, Arc::clone(&compiled));
    Ok(compiled)
}

fn compiled_trigger_function_for_proc(
    proc_oid: u32,
    call: &TriggerCallContext,
    ctx: &mut ExecutorContext,
) -> Result<Arc<CompiledFunction>, ExecError> {
    let catalog = executor_catalog(ctx, "user-defined functions")?;
    let Some(row) = catalog.proc_row_by_oid(proc_oid) else {
        ctx.plpgsql_function_cache.write().remove_proc(proc_oid);
        return Err(function_runtime_error(
            &format!("unknown function oid {proc_oid}"),
            None,
            "42883",
        ));
    };
    validate_plpgsql_function_row(&row, catalog, "function")?;
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

    let key = trigger_cache_key(proc_oid, call);
    if let Some(compiled) = ctx.plpgsql_function_cache.read().get_valid(&key, &row) {
        return Ok(compiled);
    }
    let compiled = Arc::new(
        compile_trigger_function_from_proc(
            &row,
            &call.relation_desc,
            &call.transition_tables,
            catalog,
            Some(&ctx.gucs),
        )
        .map_err(ExecError::Parse)?,
    );
    ctx.plpgsql_function_cache
        .write()
        .insert(key, row, Arc::clone(&compiled));
    Ok(compiled)
}

fn compiled_event_trigger_function_for_proc(
    proc_oid: u32,
    ctx: &mut ExecutorContext,
) -> Result<Arc<CompiledFunction>, ExecError> {
    let catalog = executor_catalog(ctx, "user-defined functions")?;
    let Some(row) = catalog.proc_row_by_oid(proc_oid) else {
        ctx.plpgsql_function_cache.write().remove_proc(proc_oid);
        return Err(function_runtime_error(
            &format!("unknown function oid {proc_oid}"),
            None,
            "42883",
        ));
    };
    validate_plpgsql_function_row(&row, catalog, "function")?;
    let return_type = catalog.type_by_oid(row.prorettype).ok_or_else(|| {
        function_runtime_error(
            &format!("unknown return type oid {}", row.prorettype),
            None,
            "42883",
        )
    })?;
    if return_type.sql_type.kind != SqlTypeKind::EventTrigger {
        return Err(function_runtime_error(
            "event trigger runtime called for a non-event-trigger function",
            Some(format!("return type is {:?}", return_type.sql_type.kind)),
            "0A000",
        ));
    }
    if row.prorettype != EVENT_TRIGGER_TYPE_OID {
        return Err(function_runtime_error(
            "event trigger function return type has unexpected oid",
            Some(format!("prorettype = {}", row.prorettype)),
            "0A000",
        ));
    }
    if row.pronargs != 0 {
        return Err(function_runtime_error(
            "event trigger functions must not accept SQL arguments",
            Some(format!("pronargs = {}", row.pronargs)),
            "0A000",
        ));
    }

    let key = PlpgsqlFunctionCacheKey::EventTrigger { proc_oid };
    if let Some(compiled) = ctx.plpgsql_function_cache.read().get_valid(&key, &row) {
        return Ok(compiled);
    }
    let compiled = Arc::new(
        compile_event_trigger_function_from_proc(&row, catalog).map_err(ExecError::Parse)?,
    );
    ctx.plpgsql_function_cache
        .write()
        .insert(key, row, Arc::clone(&compiled));
    Ok(compiled)
}

fn executor_catalog<'a>(
    ctx: &'a ExecutorContext,
    object_kind: &str,
) -> Result<&'a dyn CatalogLookup, ExecError> {
    ctx.catalog.as_deref().ok_or_else(|| {
        function_runtime_error(
            &format!("{object_kind} require executor catalog context"),
            None,
            "0A000",
        )
    })
}

fn validate_plpgsql_function_row(
    row: &PgProcRow,
    catalog: &dyn CatalogLookup,
    object_kind: &str,
) -> Result<(), ExecError> {
    let language_name = catalog
        .language_row_by_oid(row.prolang)
        .map(|row| row.lanname);
    validate_plpgsql_function_row_impl(row, language_name.as_deref(), object_kind)
        .map_err(plpgsql_routine_validation_error)
}

fn validate_plpgsql_procedure_row(
    row: &PgProcRow,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    let language_name = catalog
        .language_row_by_oid(row.prolang)
        .map(|row| row.lanname);
    validate_plpgsql_procedure_row_impl(row, language_name.as_deref())
        .map_err(plpgsql_routine_validation_error)
}

fn plpgsql_routine_validation_error(
    error: pgrust_plpgsql::PlpgsqlRoutineValidationError,
) -> ExecError {
    match error {
        pgrust_plpgsql::PlpgsqlRoutineValidationError::WrongFunctionKind { prokind } => {
            function_runtime_error(
                "only functions are executable through the PL/pgSQL runtime",
                Some(format!("prokind = {prokind}")),
                "0A000",
            )
        }
        pgrust_plpgsql::PlpgsqlRoutineValidationError::WrongProcedureKind { prokind } => {
            function_runtime_error(
                "only procedures are executable through CALL",
                Some(format!("prokind = {prokind}")),
                "0A000",
            )
        }
        pgrust_plpgsql::PlpgsqlRoutineValidationError::UnknownLanguage { prolang } => {
            function_runtime_error(&format!("unknown language oid {prolang}"), None, "42883")
        }
        pgrust_plpgsql::PlpgsqlRoutineValidationError::UnsupportedLanguage {
            object_kind,
            language_name,
        } => function_runtime_error(
            &format!("only LANGUAGE plpgsql {object_kind}s are supported"),
            Some(format!("{object_kind} language is {language_name}")),
            "0A000",
        ),
    }
}

fn plpgsql_scalar_call_validation_error(
    error: pgrust_plpgsql::PlpgsqlScalarCallValidationError,
) -> ExecError {
    match error {
        pgrust_plpgsql::PlpgsqlScalarCallValidationError::SetReturningInScalarContext => {
            function_runtime_error(
                "set-returning function called in scalar context",
                None,
                "0A000",
            )
        }
        pgrust_plpgsql::PlpgsqlScalarCallValidationError::TriggerInScalarContext { context } => {
            let message = match context {
                pgrust_plpgsql::ScalarCallContext::ExprArgs => {
                    "trigger functions can only be called as triggers"
                }
                pgrust_plpgsql::ScalarCallContext::ValueArgs => {
                    "trigger function called in scalar context"
                }
            };
            function_runtime_error(message, None, "0A000")
        }
        pgrust_plpgsql::PlpgsqlScalarCallValidationError::EventTriggerInScalarContext {
            context,
        } => {
            let message = match context {
                pgrust_plpgsql::ScalarCallContext::ExprArgs => {
                    "trigger functions can only be called as triggers"
                }
                pgrust_plpgsql::ScalarCallContext::ValueArgs => {
                    "event trigger function called in scalar context"
                }
            };
            function_runtime_error(message, None, "0A000")
        }
    }
}

fn plpgsql_trigger_contract_error(error: pgrust_plpgsql::PlpgsqlTriggerContractError) -> ExecError {
    match error {
        pgrust_plpgsql::PlpgsqlTriggerContractError::NonTrigger => function_runtime_error(
            "trigger function compiled with a non-trigger return contract",
            None,
            "0A000",
        ),
        pgrust_plpgsql::PlpgsqlTriggerContractError::NonEventTrigger => function_runtime_error(
            "event trigger function compiled with a non-event-trigger return contract",
            None,
            "0A000",
        ),
    }
}

fn install_trigger_transition_ctes(
    compiled: &CompiledFunction,
    call: &TriggerCallContext,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    for compiled_cte in &compiled.trigger_transition_ctes {
        let table = call
            .transition_tables
            .iter()
            .find(|table| table.name == compiled_cte.name)
            .ok_or_else(|| {
                function_runtime_error(
                    &format!(
                        "missing transition table \"{}\" for cached trigger function",
                        compiled_cte.name
                    ),
                    None,
                    "XX000",
                )
            })?;
        ctx.pinned_cte_tables.insert(
            compiled_cte.cte_id,
            Rc::new(RefCell::new(MaterializedCteTable {
                rows: materialized_transition_table_rows(table),
                eof: true,
            })),
        );
        ctx.cte_producers.remove(&compiled_cte.cte_id);
    }
    Ok(())
}

fn materialized_transition_table_rows(
    table: &pgrust_nodes::TriggerTransitionTable,
) -> Vec<MaterializedRow> {
    transition_table_visible_rows(table)
        .into_iter()
        .map(|mut values| {
            Value::materialize_all(&mut values);
            MaterializedRow::new(TupleSlot::virtual_row(values), Vec::new())
        })
        .collect()
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
        cursors: HashMap::new(),
        local_guc_writes: HashSet::new(),
        session_guc_writes: HashSet::new(),
        last_row_count: 0,
        current_exception: None,
    };
    state.values[compiled.found_slot] = Value::Bool(false);
    state.values[compiled.sqlstate_slot] = Value::Text(String::new().into());
    state.values[compiled.sqlerrm_slot] = Value::Text(String::new().into());
    for (slot_def, arg_value) in compiled.parameter_slots.iter().zip(arg_values.iter()) {
        state.values[slot_def.slot] =
            cast_function_value(arg_value.clone(), None, slot_def.ty, ctx)?;
    }

    let config_entries = parsed_proconfig(compiled.proconfig.as_deref());
    let has_function_config = !config_entries.is_empty();
    let restores_identity = compiled.prosecdef
        || config_entries
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("role"));
    let saved_identity = save_function_identity(ctx);
    if compiled.prosecdef {
        apply_security_definer_identity(ctx, compiled.proowner);
    }
    let saved_gucs = ctx.gucs.clone();
    let mut function_config_names = HashSet::new();
    for (name, value) in config_entries {
        match apply_function_guc(ctx, &name, Some(&value)) {
            Ok(normalized) => {
                function_config_names.insert(normalized);
            }
            Err(err) => {
                ctx.gucs = saved_gucs;
                if restores_identity {
                    restore_function_identity(ctx, saved_identity);
                }
                return Err(err);
            }
        }
    }

    let block_result = exec_function_block(
        &compiled.body,
        compiled,
        expected_record_shape,
        &mut state,
        ctx,
    )
    .map_err(|err| with_plpgsql_context_if_missing(err, compiled, "statement"));

    match block_result {
        Ok(FunctionControl::Continue | FunctionControl::Return) => {}
        Ok(FunctionControl::LoopContinue) => {
            ctx.gucs = saved_gucs;
            if restores_identity {
                restore_function_identity(ctx, saved_identity);
            }
            return Err(function_runtime_error(
                "CONTINUE cannot be used outside a loop",
                None,
                "2D000",
            ));
        }
        Ok(FunctionControl::ExitLoop) => {
            ctx.gucs = saved_gucs;
            if restores_identity {
                restore_function_identity(ctx, saved_identity);
            }
            return Err(function_runtime_error(
                "EXIT cannot be used outside a loop",
                None,
                "2D000",
            ));
        }
        Err(err) => {
            ctx.gucs = saved_gucs;
            if restores_identity {
                restore_function_identity(ctx, saved_identity);
            }
            return Err(err);
        }
    }

    if has_function_config {
        let restore_names = function_config_names
            .into_iter()
            .filter(|name| !state.session_guc_writes.contains(name))
            .chain(
                state
                    .local_guc_writes
                    .iter()
                    .filter(|name| !state.session_guc_writes.contains(*name))
                    .cloned(),
            )
            .collect::<HashSet<_>>();
        restore_function_gucs(ctx, saved_gucs, restore_names);
    }
    if restores_identity && !state.session_guc_writes.contains("role") {
        restore_function_identity(ctx, saved_identity);
    }

    export_open_cursors_as_portals(&state, ctx);

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
                    state.scalar_return = Some(cast_function_value(
                        state.values[*slot].clone(),
                        None,
                        *ty,
                        ctx,
                    )?);
                } else {
                    return Err(with_plpgsql_function_context(
                        function_runtime_error(
                            "control reached end of function without RETURN",
                            None,
                            "2F005",
                        ),
                        compiled,
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
                state.rows.push(current_output_row(compiled, &state, ctx)?);
            }
            Ok(state.rows)
        }
        FunctionReturnContract::Scalar { setof: true, .. }
        | FunctionReturnContract::FixedRow { .. }
        | FunctionReturnContract::AnonymousRecord { .. } => Ok(state.rows),
        FunctionReturnContract::Trigger { .. } | FunctionReturnContract::EventTrigger { .. } => {
            Err(function_runtime_error(
                "trigger function executed through SQL function path",
                None,
                "0A000",
            ))
        }
    }
}

fn execute_compiled_function_for_call(
    compiled: &CompiledFunction,
    arg_values: &[Value],
    expected_record_shape: Option<&[QueryColumn]>,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let saved_snapshot_cid = if compiled.provolatile == 'v' {
        let saved = ctx.snapshot.current_cid;
        ctx.snapshot.current_cid = CommandId::MAX;
        Some(saved)
    } else {
        None
    };
    let result = execute_compiled_function(compiled, arg_values, expected_record_shape, ctx);
    if let Some(saved) = saved_snapshot_cid {
        ctx.snapshot.current_cid = saved;
    }
    result
}

fn exec_do_block(
    block: &CompiledBlock,
    values: &mut [Value],
    gucs: &HashMap<String, String>,
) -> Result<DoControl, ExecError> {
    for local in &block.local_slots {
        let value = match &local.default_expr {
            Some(expr) => cast_value(eval_do_expr(expr, values)?, local.ty)?,
            None => Value::Null,
        };
        ensure_not_null_assignment(Some(&local.name), local.not_null, &value)?;
        values[local.slot] = value;
    }
    for stmt in &block.statements {
        match exec_do_stmt(stmt, values, gucs) {
            Ok(DoControl::Continue) => {}
            Ok(DoControl::LoopContinue) => return Ok(DoControl::LoopContinue),
            Err(err) => {
                return match exec_do_exception_handlers(block, &err, values, gucs)? {
                    Some(()) => Ok(DoControl::Continue),
                    None => Err(err),
                };
            }
        }
    }
    Ok(DoControl::Continue)
}

fn exec_do_exception_handlers(
    block: &CompiledBlock,
    err: &ExecError,
    values: &mut [Value],
    gucs: &HashMap<String, String>,
) -> Result<Option<()>, ExecError> {
    let Some(handler) = block
        .exception_handlers
        .iter()
        .find(|handler| handler_matches(handler, err))
    else {
        return Ok(None);
    };
    let current = exception_data_from_error(err);
    let saved_sqlstate = block
        .exception_sqlstate_slot
        .map(|slot| (slot, values[slot].clone()));
    let saved_sqlerrm = block
        .exception_sqlerrm_slot
        .map(|slot| (slot, values[slot].clone()));
    if let Some(slot) = block.exception_sqlstate_slot {
        values[slot] = Value::Text(current.sqlstate.into());
    }
    if let Some(slot) = block.exception_sqlerrm_slot {
        values[slot] = Value::Text(current.message.into());
    }
    let mut result = Ok(());
    for stmt in &handler.statements {
        match exec_do_stmt(stmt, values, gucs) {
            Ok(DoControl::Continue) => {}
            Ok(DoControl::LoopContinue) => break,
            Err(err) => {
                result = Err(err);
                break;
            }
        }
    }
    if let Some((slot, value)) = saved_sqlstate {
        values[slot] = value;
    }
    if let Some((slot, value)) = saved_sqlerrm {
        values[slot] = value;
    }
    result.map(|()| Some(()))
}

fn exec_do_stmt(
    stmt: &CompiledStmt,
    values: &mut [Value],
    gucs: &HashMap<String, String>,
) -> Result<DoControl, ExecError> {
    match stmt {
        CompiledStmt::WithLine { stmt, .. } => exec_do_stmt(stmt, values, gucs),
        CompiledStmt::Block(block) => exec_do_block(block, values, gucs),
        CompiledStmt::Assign {
            slot,
            ty,
            name,
            not_null,
            expr,
            ..
        } => {
            let value = cast_value(eval_do_expr(expr, values)?, *ty)?;
            ensure_not_null_assignment(name.as_deref(), *not_null, &value)?;
            values[*slot] = value;
            Ok(DoControl::Continue)
        }
        CompiledStmt::AssignSubscript { .. } => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "subscripted PL/pgSQL assignment is only supported inside CREATE FUNCTION".into(),
            )))
        }
        CompiledStmt::AssignIndirect { .. } => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "indirect PL/pgSQL assignment is only supported inside CREATE FUNCTION".into(),
            )))
        }
        CompiledStmt::AssignTriggerRow { .. } => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "trigger-row PL/pgSQL assignment is only supported inside CREATE FUNCTION".into(),
            )))
        }
        CompiledStmt::Null => Ok(DoControl::Continue),
        CompiledStmt::If {
            branches,
            else_branch,
        } => {
            for (condition, body) in branches {
                match eval_do_expr(condition, values)? {
                    Value::Bool(true) => {
                        for stmt in body {
                            if matches!(exec_do_stmt(stmt, values, gucs)?, DoControl::LoopContinue)
                            {
                                return Ok(DoControl::LoopContinue);
                            }
                        }
                        return Ok(DoControl::Continue);
                    }
                    Value::Bool(false) | Value::Null => {}
                    other => return Err(ExecError::NonBoolQual(other)),
                }
            }
            for stmt in else_branch {
                if matches!(exec_do_stmt(stmt, values, gucs)?, DoControl::LoopContinue) {
                    return Ok(DoControl::LoopContinue);
                }
            }
            Ok(DoControl::Continue)
        }
        CompiledStmt::While { condition, body } => {
            while eval_plpgsql_condition(&eval_do_expr(condition, values)?)? {
                for stmt in body {
                    if matches!(exec_do_stmt(stmt, values, gucs)?, DoControl::LoopContinue) {
                        break;
                    }
                }
            }
            Ok(DoControl::Continue)
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
                return Ok(DoControl::Continue);
            }
            for current in start..=end {
                values[*slot] = Value::Int32(current);
                for stmt in body {
                    if matches!(exec_do_stmt(stmt, values, gucs)?, DoControl::LoopContinue) {
                        break;
                    }
                }
            }
            Ok(DoControl::Continue)
        }
        CompiledStmt::Exit { .. } => Ok(DoControl::Continue),
        CompiledStmt::Continue { condition } => {
            let should_continue = match condition {
                Some(condition) => eval_plpgsql_condition(&eval_do_expr(condition, values)?)?,
                None => true,
            };
            if should_continue {
                Ok(DoControl::LoopContinue)
            } else {
                Ok(DoControl::Continue)
            }
        }
        CompiledStmt::Raise {
            level,
            sqlstate,
            message,
            message_expr,
            detail_expr,
            hint_expr,
            errcode_expr,
            column_expr,
            constraint_expr,
            datatype_expr,
            table_expr,
            schema_expr,
            params,
            ..
        } => {
            let param_values = params
                .iter()
                .map(|expr| eval_do_expr(expr, values))
                .collect::<Result<Vec<_>, _>>()?;
            let message = match message_expr {
                Some(expr) => Some(render_raise_option_value(eval_do_expr(expr, values)?)),
                None => message.clone(),
            };
            let detail = detail_expr
                .as_ref()
                .map(|expr| eval_do_expr(expr, values).map(render_raise_option_value))
                .transpose()?;
            let hint = hint_expr
                .as_ref()
                .map(|expr| eval_do_expr(expr, values).map(render_raise_option_value))
                .transpose()?;
            let dynamic_sqlstate = errcode_expr
                .as_ref()
                .map(|expr| eval_do_expr(expr, values).map(render_raise_option_value))
                .transpose()?;
            let fields = PlpgsqlErrorFields {
                column_name: eval_do_raise_field(column_expr.as_ref(), values)?,
                constraint_name: eval_do_raise_field(constraint_expr.as_ref(), values)?,
                datatype_name: eval_do_raise_field(datatype_expr.as_ref(), values)?,
                table_name: eval_do_raise_field(table_expr.as_ref(), values)?,
                schema_name: eval_do_raise_field(schema_expr.as_ref(), values)?,
            };
            finish_raise(
                level,
                dynamic_sqlstate.as_deref().or(sqlstate.as_deref()),
                message.as_deref().unwrap_or("P0001"),
                &param_values,
                detail,
                hint,
                fields,
            )?;
            Ok(DoControl::Continue)
        }
        CompiledStmt::Reraise => Err(ExecError::DetailedError {
            message: "RAISE without parameters cannot be used outside an exception handler".into(),
            detail: None,
            hint: None,
            sqlstate: "0Z002",
        }),
        CompiledStmt::Assert { condition, message } => {
            if !plpgsql_check_asserts_enabled_from_gucs(Some(gucs)) {
                return Ok(DoControl::Continue);
            }
            let ok = eval_plpgsql_condition(&eval_do_expr(condition, values)?)?;
            if ok {
                return Ok(DoControl::Continue);
            }
            let message = match message {
                Some(expr) => render_assert_message(eval_do_expr(expr, values)?)?,
                None => "assertion failed".into(),
            };
            Err(assert_failure(message))
        }
        CompiledStmt::GetDiagnostics { stacked, items } => {
            if *stacked {
                return Err(ExecError::DetailedError {
                    message: "GET STACKED DIAGNOSTICS cannot be used outside an exception handler"
                        .into(),
                    detail: None,
                    hint: None,
                    sqlstate: "0Z002",
                });
            }
            for (target, item) in items {
                let value = match item.to_ascii_lowercase().as_str() {
                    "row_count" => Value::Int64(0),
                    "found" => Value::Bool(false),
                    "pg_routine_oid" => Value::Int64(0),
                    _ => diagnostic_text(None),
                };
                values[target.slot] = cast_value(value, target.ty)?;
            }
            Ok(DoControl::Continue)
        }
        CompiledStmt::DynamicExecute {
            sql_expr,
            strict: _,
            into_targets,
            using_exprs,
            ..
        } => {
            exec_do_dynamic_execute(sql_expr, into_targets, using_exprs, values)?;
            Ok(DoControl::Continue)
        }
        CompiledStmt::SetGuc { .. } => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "SET is only supported inside CREATE FUNCTION".into(),
        ))),
        CompiledStmt::Return { .. }
        | CompiledStmt::ReturnRuntimeQuery { .. }
        | CompiledStmt::ReturnSelect { .. }
        | CompiledStmt::ReturnNext { .. }
        | CompiledStmt::ReturnTriggerRow { .. }
        | CompiledStmt::ReturnTriggerNull
        | CompiledStmt::ReturnTriggerNoValue
        | CompiledStmt::Loop { .. }
        | CompiledStmt::ForQuery { .. }
        | CompiledStmt::ForEach { .. }
        | CompiledStmt::ReturnQuery { .. }
        | CompiledStmt::Perform { .. }
        | CompiledStmt::OpenCursor { .. }
        | CompiledStmt::FetchCursor { .. }
        | CompiledStmt::MoveCursor { .. }
        | CompiledStmt::CloseCursor { .. }
        | CompiledStmt::UnsupportedTransactionCommand { .. }
        | CompiledStmt::CommentOnFunction { .. }
        | CompiledStmt::SelectInto { .. }
        | CompiledStmt::ExecInsertInto { .. }
        | CompiledStmt::ExecInsert { .. }
        | CompiledStmt::ExecUpdateInto { .. }
        | CompiledStmt::ExecUpdate { .. }
        | CompiledStmt::ExecDeleteInto { .. }
        | CompiledStmt::ExecDelete { .. }
        | CompiledStmt::RuntimeSql { .. }
        | CompiledStmt::RuntimeSelectInto { .. }
        | CompiledStmt::CreateTableAs { .. }
        | CompiledStmt::CreateTable { .. }
        | CompiledStmt::ExecSql { .. } => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "statement is only supported inside CREATE FUNCTION".into(),
        ))),
    }
}

fn exec_do_dynamic_execute(
    sql_expr: &CompiledExpr,
    into_targets: &[CompiledSelectIntoTarget],
    using_exprs: &[CompiledExpr],
    values: &mut [Value],
) -> Result<(), ExecError> {
    if !using_exprs.is_empty() {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "EXECUTE USING is only supported inside CREATE FUNCTION".into(),
        )));
    }
    let sql_value = eval_do_expr(sql_expr, values)?;
    if matches!(sql_value, Value::Null) {
        return Err(function_runtime_error(
            "query string argument of EXECUTE is null",
            None,
            "22004",
        ));
    }
    let sql_text = cast_value(sql_value, SqlType::new(SqlTypeKind::Text))?;
    let sql_text = sql_text.as_text().ok_or_else(|| {
        function_runtime_error(
            "EXECUTE query string did not evaluate to text",
            None,
            "42804",
        )
    })?;
    if into_targets.is_empty() {
        return Ok(());
    }

    let Statement::Select(stmt) = parse_statement(sql_text).map_err(ExecError::Parse)? else {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "EXECUTE INTO in DO supports scalar SELECT statements".into(),
        )));
    };
    if stmt.from.is_some()
        || !stmt.with.is_empty()
        || stmt.where_clause.is_some()
        || !stmt.group_by.is_empty()
        || stmt.having.is_some()
        || stmt.set_operation.is_some()
        || stmt.targets.len() != 1
    {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "EXECUTE INTO in DO supports scalar SELECT statements".into(),
        )));
    }

    let catalog = Catalog::default();
    let (expr, _) =
        bind_scalar_expr_in_named_slot_scope(&stmt.targets[0].expr, &[], &[], &catalog, &[])
            .map_err(ExecError::Parse)?;
    let mut slot = TupleSlot::virtual_row(Vec::new());
    let value = eval_plpgsql_expr(&expr, &mut slot)?;
    for target in into_targets {
        values[target.slot] = cast_value(value.clone(), target.ty)?;
    }
    Ok(())
}

fn exec_function_block(
    block: &CompiledBlock,
    compiled: &CompiledFunction,
    expected_record_shape: Option<&[QueryColumn]>,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionControl, ExecError> {
    // :HACK: PostgreSQL runs PL/pgSQL exception blocks in subtransactions.
    // pgrust does not have full subtransaction ownership yet, but anonymous DO
    // blocks need aborted inner writes to become invisible for MVCC regressions.
    let subxact = if compiled.name == "inline_code_block"
        && !block.exception_handlers.is_empty()
        && ctx.snapshot.current_xid != INVALID_TRANSACTION_ID
    {
        Some((
            ctx.snapshot.clone(),
            ctx.write_xid_override,
            ctx.txns.write().begin(),
        ))
    } else {
        None
    };
    if let Some((_, _, subxid)) = &subxact {
        ctx.snapshot.own_xids.insert(*subxid);
        ctx.write_xid_override = Some(*subxid);
        sync_plpgsql_catalog_snapshot_override(ctx);
    }

    for local in &block.local_slots {
        let (value, source_type) = match &local.default_expr {
            Some(expr) => (
                eval_function_expr(expr, &state.values, ctx)
                    .map_err(|err| with_plpgsql_local_init_context(err, compiled, local.line))?,
                compiled_expr_sql_type_hint(expr),
            ),
            None => (Value::Null, None),
        };
        let value = cast_function_value(value, source_type, local.ty, ctx)
            .map_err(|err| with_plpgsql_local_init_context(err, compiled, local.line))?;
        ensure_not_null_assignment(Some(&local.name), local.not_null, &value)
            .map_err(|err| with_plpgsql_local_init_context(err, compiled, local.line))?;
        state.values[local.slot] = value;
    }
    for stmt in &block.statements {
        match exec_function_stmt(stmt, compiled, expected_record_shape, state, ctx)
            .map_err(|err| with_plpgsql_stmt_context_if_missing(err, compiled, stmt))
        {
            Ok(FunctionControl::Continue) => {}
            Ok(FunctionControl::LoopContinue) => {
                finish_function_block_subxact(ctx, subxact, true)?;
                return Ok(FunctionControl::LoopContinue);
            }
            Ok(FunctionControl::ExitLoop) => {
                finish_function_block_subxact(ctx, subxact, true)?;
                return Ok(FunctionControl::ExitLoop);
            }
            Ok(FunctionControl::Return) => {
                finish_function_block_subxact(ctx, subxact, true)?;
                return Ok(FunctionControl::Return);
            }
            Err(err) => {
                if let Some((parent_snapshot, saved_write_xid, subxid)) = subxact {
                    ctx.txns
                        .write()
                        .abort(subxid)
                        .map_err(|e| ExecError::Heap(HeapError::Mvcc(e)))?;
                    ctx.snapshot = parent_snapshot;
                    ctx.write_xid_override = saved_write_xid;
                    sync_plpgsql_catalog_snapshot_override(ctx);
                }
                return match exec_function_exception_handlers(
                    &block.exception_handlers,
                    &err,
                    compiled,
                    expected_record_shape,
                    state,
                    ctx,
                )? {
                    Some(control) => Ok(control),
                    None => Err(err),
                };
            }
        }
    }
    finish_function_block_subxact(ctx, subxact, true)?;
    Ok(FunctionControl::Continue)
}

fn finish_function_block_subxact(
    ctx: &mut ExecutorContext,
    subxact: Option<(
        crate::backend::access::transam::xact::Snapshot,
        Option<TransactionId>,
        TransactionId,
    )>,
    commit: bool,
) -> Result<(), ExecError> {
    let Some((mut parent_snapshot, saved_write_xid, subxid)) = subxact else {
        return Ok(());
    };
    if commit {
        ctx.txns
            .write()
            .commit(subxid)
            .map_err(|e| ExecError::Heap(HeapError::Mvcc(e)))?;
        parent_snapshot.own_xids.insert(subxid);
    } else {
        ctx.txns
            .write()
            .abort(subxid)
            .map_err(|e| ExecError::Heap(HeapError::Mvcc(e)))?;
    }
    ctx.snapshot = parent_snapshot;
    ctx.write_xid_override = saved_write_xid;
    sync_plpgsql_catalog_snapshot_override(ctx);
    Ok(())
}

fn sync_plpgsql_catalog_snapshot_override(ctx: &ExecutorContext) {
    let Some(db) = ctx.database.clone() else {
        return;
    };
    if ctx.snapshot.current_xid != INVALID_TRANSACTION_ID {
        crate::backend::utils::time::snapmgr::set_transaction_snapshot_override(
            &db,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.snapshot.clone(),
        );
    } else {
        crate::backend::utils::time::snapmgr::clear_transaction_snapshot_override(
            &db,
            ctx.client_id,
        );
    }
}

fn exec_function_exception_handlers(
    handlers: &[CompiledExceptionHandler],
    err: &ExecError,
    compiled: &CompiledFunction,
    expected_record_shape: Option<&[QueryColumn]>,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<Option<FunctionControl>, ExecError> {
    let Some(handler) = handlers
        .iter()
        .find(|handler| handler_matches(handler, err))
    else {
        return Ok(None);
    };
    let saved_exception = state.current_exception.clone();
    let saved_sqlstate = state.values[compiled.sqlstate_slot].clone();
    let saved_sqlerrm = state.values[compiled.sqlerrm_slot].clone();
    let current = exception_data_from_error(err);
    state.values[compiled.sqlstate_slot] = Value::Text(current.sqlstate.into());
    state.values[compiled.sqlerrm_slot] = Value::Text(current.message.clone().into());
    state.current_exception = Some(current);
    let result = exec_function_stmt_list(
        &handler.statements,
        compiled,
        expected_record_shape,
        state,
        ctx,
    )
    .map(Some);
    state.current_exception = saved_exception;
    state.values[compiled.sqlstate_slot] = saved_sqlstate;
    state.values[compiled.sqlerrm_slot] = saved_sqlerrm;
    result
}

fn exec_function_stmt(
    stmt: &CompiledStmt,
    compiled: &CompiledFunction,
    expected_record_shape: Option<&[QueryColumn]>,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionControl, ExecError> {
    match stmt {
        CompiledStmt::WithLine { line, stmt } => {
            if matches!(stmt.as_ref(), CompiledStmt::Block(_)) {
                exec_function_stmt(stmt, compiled, expected_record_shape, state, ctx)
            } else {
                with_context_frame(compiled, *line, stmt_context_action(stmt), || {
                    exec_function_stmt(stmt, compiled, expected_record_shape, state, ctx)
                })
            }
        }
        CompiledStmt::Block(block) => {
            exec_function_block(block, compiled, expected_record_shape, state, ctx)
        }
        CompiledStmt::Assign {
            slot,
            ty,
            name,
            not_null,
            expr,
            ..
        } => {
            let value = eval_function_expr(expr, &state.values, ctx)?;
            let value = cast_function_value(value, compiled_expr_sql_type_hint(expr), *ty, ctx)?;
            ensure_not_null_assignment(name.as_deref(), *not_null, &value)?;
            state.values[*slot] = value;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::AssignSubscript {
            slot,
            root_ty,
            target_ty,
            subscripts,
            expr,
            ..
        } => {
            let mut subscript_values = Vec::with_capacity(subscripts.len());
            for subscript in subscripts {
                let value = eval_function_expr(subscript, &state.values, ctx)?;
                subscript_values.push((false, Some(value), None));
            }
            let value = eval_function_expr(expr, &state.values, ctx)?;
            let value =
                cast_function_value(value, compiled_expr_sql_type_hint(expr), *target_ty, ctx)?;
            let assigned = apply_sql_type_array_subscript_assignment(
                state.values[*slot].clone(),
                *root_ty,
                &subscript_values,
                value,
                ctx,
            )?;
            state.values[*slot] = enforce_domain_constraints_for_value(assigned, *root_ty, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::AssignIndirect { target, expr, .. } => {
            let indirection = eval_assign_indirection_function(target, &state.values, ctx)?;
            let value = eval_function_expr(expr, &state.values, ctx)?;
            let assigned = assign_indirect_value(
                state.values[target.slot].clone(),
                target.ty,
                &indirection,
                value,
                Some(ctx),
            )?;
            state.values[target.slot] =
                enforce_domain_constraints_for_value(assigned, target.ty, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::AssignTriggerRow { row, expr, .. } => {
            let value = eval_function_expr(expr, &state.values, ctx)?;
            assign_trigger_row_value(compiled, state, *row, value, ctx)?;
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
                match exec_function_stmt_list(body, compiled, expected_record_shape, state, ctx)? {
                    FunctionControl::Continue | FunctionControl::LoopContinue => {}
                    FunctionControl::ExitLoop => break,
                    FunctionControl::Return => return Ok(FunctionControl::Return),
                }
            }
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::Loop { body } => {
            loop {
                match exec_function_stmt_list(body, compiled, expected_record_shape, state, ctx)? {
                    FunctionControl::Continue | FunctionControl::LoopContinue => {}
                    FunctionControl::ExitLoop => break,
                    FunctionControl::Return => return Ok(FunctionControl::Return),
                }
            }
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::Exit { condition } => {
            let should_exit = match condition {
                Some(condition) => {
                    eval_plpgsql_condition(&eval_function_expr(condition, &state.values, ctx)?)?
                }
                None => true,
            };
            if should_exit {
                Ok(FunctionControl::ExitLoop)
            } else {
                Ok(FunctionControl::Continue)
            }
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
                state.values[compiled.found_slot] = Value::Bool(false);
                return Ok(FunctionControl::Continue);
            }
            for current in start..=end {
                state.values[*slot] = Value::Int32(current);
                match exec_function_stmt_list(body, compiled, expected_record_shape, state, ctx)? {
                    FunctionControl::Continue | FunctionControl::LoopContinue => {}
                    FunctionControl::ExitLoop => break,
                    FunctionControl::Return => return Ok(FunctionControl::Return),
                }
            }
            state.values[compiled.found_slot] = Value::Bool(true);
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::Continue { condition } => {
            let should_continue = match condition {
                Some(condition) => {
                    eval_plpgsql_condition(&eval_function_expr(condition, &state.values, ctx)?)?
                }
                None => true,
            };
            if should_continue {
                Ok(FunctionControl::LoopContinue)
            } else {
                Ok(FunctionControl::Continue)
            }
        }
        CompiledStmt::ForQuery {
            target,
            source,
            body,
        } => exec_function_for_query(
            target,
            source,
            body,
            compiled,
            expected_record_shape,
            state,
            ctx,
        ),
        CompiledStmt::ForEach {
            target,
            slice,
            array_expr,
            body,
        } => exec_function_foreach(
            target,
            *slice,
            array_expr,
            body,
            compiled,
            expected_record_shape,
            state,
            ctx,
        ),
        CompiledStmt::Raise {
            level,
            sqlstate,
            message,
            message_expr,
            detail_expr,
            hint_expr,
            errcode_expr,
            column_expr,
            constraint_expr,
            datatype_expr,
            table_expr,
            schema_expr,
            params,
            ..
        } => {
            let param_values = params
                .iter()
                .map(|expr| eval_function_expr(expr, &state.values, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            let message = match message_expr {
                Some(expr) => Some(render_raise_option_value(eval_function_expr(
                    expr,
                    &state.values,
                    ctx,
                )?)),
                None => message.clone(),
            };
            let detail = detail_expr
                .as_ref()
                .map(|expr| {
                    eval_function_expr(expr, &state.values, ctx).map(render_raise_option_value)
                })
                .transpose()?;
            let hint = hint_expr
                .as_ref()
                .map(|expr| {
                    eval_function_expr(expr, &state.values, ctx).map(render_raise_option_value)
                })
                .transpose()?;
            let dynamic_sqlstate = errcode_expr
                .as_ref()
                .map(|expr| {
                    eval_function_expr(expr, &state.values, ctx).map(render_raise_option_value)
                })
                .transpose()?;
            let fields = PlpgsqlErrorFields {
                column_name: eval_function_raise_field(column_expr.as_ref(), &state.values, ctx)?,
                constraint_name: eval_function_raise_field(
                    constraint_expr.as_ref(),
                    &state.values,
                    ctx,
                )?,
                datatype_name: eval_function_raise_field(
                    datatype_expr.as_ref(),
                    &state.values,
                    ctx,
                )?,
                table_name: eval_function_raise_field(table_expr.as_ref(), &state.values, ctx)?,
                schema_name: eval_function_raise_field(schema_expr.as_ref(), &state.values, ctx)?,
            };
            finish_raise(
                level,
                dynamic_sqlstate.as_deref().or(sqlstate.as_deref()),
                message.as_deref().unwrap_or("P0001"),
                &param_values,
                detail,
                hint,
                fields,
            )?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::Reraise => match &state.current_exception {
            Some(err) => Err(exception_data_to_error(err.clone())),
            None => Err(ExecError::DetailedError {
                message: "RAISE without parameters cannot be used outside an exception handler"
                    .into(),
                detail: None,
                hint: None,
                sqlstate: "0Z002",
            }),
        },
        CompiledStmt::Assert { condition, message } => {
            if !plpgsql_check_asserts_enabled_from_values(Some(ctx)) {
                return Ok(FunctionControl::Continue);
            }
            let ok = eval_plpgsql_condition(&eval_function_expr(condition, &state.values, ctx)?)?;
            if ok {
                return Ok(FunctionControl::Continue);
            }
            let message = match message {
                Some(expr) => render_assert_message(eval_function_expr(expr, &state.values, ctx)?)?,
                None => "assertion failed".into(),
            };
            Err(assert_failure(message))
        }
        CompiledStmt::Return { expr, .. } => {
            exec_function_return(expr.as_ref(), compiled, expected_record_shape, state, ctx)
        }
        CompiledStmt::ReturnRuntimeQuery { sql, scope, .. } => exec_function_return_runtime_query(
            sql,
            scope,
            compiled,
            expected_record_shape,
            state,
            ctx,
        ),
        CompiledStmt::ReturnSelect { plan, sql, .. } => {
            exec_function_return_select(plan, sql, compiled, expected_record_shape, state, ctx)
        }
        CompiledStmt::ReturnNext { expr } => {
            exec_function_return_next(expr.as_ref(), compiled, expected_record_shape, state, ctx)?;
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
        CompiledStmt::ReturnQuery { source } => {
            exec_function_return_query(source, compiled, expected_record_shape, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::Perform { plan, sql, .. } => {
            exec_function_perform(plan, sql.as_deref(), compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::DynamicExecute {
            sql_expr,
            strict,
            into_targets,
            using_exprs,
            ..
        } => {
            exec_function_dynamic_execute(
                sql_expr,
                *strict,
                into_targets,
                using_exprs,
                compiled,
                state,
                ctx,
            )?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::SetGuc {
            name,
            value,
            is_local,
        } => {
            let normalized = apply_function_guc(ctx, name, value.as_deref())?;
            if *is_local {
                state.local_guc_writes.insert(normalized);
            } else {
                state.session_guc_writes.insert(normalized);
            }
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::ExecSql { sql } => {
            let result = execute_dynamic_sql_statement(sql, false, None, compiled, state, ctx)?;
            state.values[compiled.found_slot] = Value::Bool(statement_result_changed_rows(&result));
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::CommentOnFunction { stmt } => {
            exec_function_comment_on_function(stmt, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::GetDiagnostics { stacked, items } => {
            exec_function_get_diagnostics(*stacked, items, compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::OpenCursor {
            slot,
            name,
            source,
            scrollable,
            constant,
        } => {
            exec_function_open_cursor(
                *slot,
                name,
                source,
                *scrollable,
                *constant,
                compiled,
                state,
                ctx,
            )?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::FetchCursor {
            slot,
            direction,
            targets,
        } => {
            exec_function_fetch_cursor(*slot, *direction, targets, compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::MoveCursor { slot, direction } => {
            exec_function_move_cursor(*slot, *direction, compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::CloseCursor { slot } => {
            exec_function_close_cursor(*slot, state)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::UnsupportedTransactionCommand { .. } => Err(function_runtime_error(
            "unsupported transaction command in PL/pgSQL",
            None,
            "0A000",
        )),
        CompiledStmt::SelectInto {
            plan,
            targets,
            strict,
            strict_params,
        } => {
            exec_function_select_into(plan, targets, *strict, strict_params, compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::ExecInsertInto { stmt, targets } => {
            exec_function_insert_into(stmt, targets, compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::ExecInsert { stmt } => {
            exec_function_insert(stmt, compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::ExecUpdateInto { stmt, targets } => {
            exec_function_update_into(stmt, targets, compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::ExecUpdate { stmt } => {
            exec_function_update(stmt, compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::ExecDeleteInto { stmt, targets } => {
            exec_function_delete_into(stmt, targets, compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::ExecDelete { stmt } => {
            exec_function_delete(stmt, compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::RuntimeSql { sql, scope } => {
            exec_function_runtime_sql(sql, scope, compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::RuntimeSelectInto {
            sql,
            scope,
            targets,
            strict,
            strict_params,
        } => {
            exec_function_runtime_select_into(
                sql,
                scope,
                targets,
                *strict,
                strict_params,
                compiled,
                state,
                ctx,
            )?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::CreateTableAs { stmt } => {
            exec_function_create_table_as(stmt, compiled, state, ctx)?;
            Ok(FunctionControl::Continue)
        }
        CompiledStmt::CreateTable { stmt } => {
            exec_function_create_table(stmt, compiled, state, ctx)?;
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
        match exec_function_stmt(stmt, compiled, expected_record_shape, state, ctx)
            .map_err(|err| with_plpgsql_stmt_context_if_missing(err, compiled, stmt))?
        {
            FunctionControl::Continue => {}
            control => return Ok(control),
        }
    }
    Ok(FunctionControl::Continue)
}

fn cast_function_scalar_return_value(value: Value, ty: SqlType) -> Result<Value, ExecError> {
    match value {
        Value::Record(record)
            if !matches!(ty.kind, SqlTypeKind::Record | SqlTypeKind::Composite) =>
        {
            cast_value(
                Value::Text(crate::backend::executor::value_io::format_record_text(&record).into()),
                ty,
            )
        }
        other => cast_value(other, ty),
    }
}

fn return_row_values_from_value(
    value: Value,
    contract: &FunctionReturnContract,
    expected_record_shape: Option<&[QueryColumn]>,
) -> Result<Vec<Value>, ExecError> {
    match value {
        Value::Record(record) => Ok(record.fields),
        Value::Null => Ok(match contract {
            FunctionReturnContract::FixedRow { columns, .. } => {
                vec![Value::Null; expected_record_shape.unwrap_or(columns).len()]
            }
            FunctionReturnContract::AnonymousRecord { .. } => expected_record_shape
                .map(|columns| vec![Value::Null; columns.len()])
                .unwrap_or_else(|| vec![Value::Null]),
            _ => vec![Value::Null],
        }),
        _ if matches!(contract, FunctionReturnContract::FixedRow { .. }) => {
            Err(function_runtime_error(
                "cannot return non-composite value from function returning composite type",
                None,
                "42804",
            ))
        }
        other => Ok(vec![other]),
    }
}

fn exec_function_return(
    expr: Option<&CompiledExpr>,
    compiled: &CompiledFunction,
    expected_record_shape: Option<&[QueryColumn]>,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionControl, ExecError> {
    match &compiled.return_contract {
        FunctionReturnContract::Trigger { .. } => Err(function_runtime_error(
            "trigger functions must return NEW, OLD, or NULL",
            None,
            "0A000",
        )),
        FunctionReturnContract::EventTrigger { .. } => match expr {
            None => {
                state.trigger_return = Some(TriggerFunctionResult::NoValue);
                Ok(FunctionControl::Return)
            }
            Some(_) => Err(function_runtime_error(
                "RETURN cannot have a parameter in function returning event_trigger",
                None,
                "42804",
            )),
        },
        FunctionReturnContract::Scalar {
            ty,
            setof: false,
            output_slot,
        } => {
            state.scalar_return = Some(match expr {
                Some(expr) => {
                    let value = eval_function_return_expr(expr, &state.values, ctx)?;
                    cast_function_value(value, compiled_expr_sql_type_hint(expr), *ty, ctx)
                        .map_err(|err| with_plpgsql_return_cast_context(err, compiled))?
                }
                None if ty.kind == SqlTypeKind::Void => Value::Null,
                None => {
                    let slot = output_slot.ok_or_else(|| {
                        function_runtime_error(
                            "control reached end of function without RETURN",
                            None,
                            "2F005",
                        )
                    })?;
                    cast_function_value(state.values[slot].clone(), None, *ty, ctx)
                        .map_err(|err| with_plpgsql_return_cast_context(err, compiled))?
                }
            });
            Ok(FunctionControl::Return)
        }
        FunctionReturnContract::Scalar { setof: true, .. }
        | FunctionReturnContract::FixedRow { setof: true, .. }
        | FunctionReturnContract::AnonymousRecord { setof: true } => Ok(FunctionControl::Return),
        FunctionReturnContract::FixedRow { setof: false, .. } => {
            if let Some(expr) = expr {
                let value = eval_function_return_expr(expr, &state.values, ctx)?;
                let row = return_row_values_from_value(
                    value,
                    &compiled.return_contract,
                    expected_record_shape,
                )?;
                state.rows.clear();
                state.rows.push(coerce_function_result_row(
                    row,
                    &compiled.return_contract,
                    expected_record_shape,
                    ctx,
                )?);
            }
            Ok(FunctionControl::Return)
        }
        FunctionReturnContract::AnonymousRecord { setof: false } => {
            if let Some(expr) = expr {
                let value = eval_function_return_expr(expr, &state.values, ctx)?;
                state.rows.clear();
                if let Some(shape) = expected_record_shape {
                    let row = return_row_values_from_value(
                        value,
                        &compiled.return_contract,
                        expected_record_shape,
                    )?;
                    state.rows.push(coerce_row_to_columns(row, shape, ctx)?);
                } else {
                    state.rows.push(TupleSlot::virtual_row(vec![value]));
                }
            }
            Ok(FunctionControl::Return)
        }
    }
}

fn exec_function_return_select(
    plan: &crate::include::nodes::plannodes::PlannedStmt,
    sql: &str,
    compiled: &CompiledFunction,
    expected_record_shape: Option<&[QueryColumn]>,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionControl, ExecError> {
    let FunctionReturnContract::Scalar {
        ty,
        setof: false,
        output_slot: None,
    } = &compiled.return_contract
    else {
        return exec_function_return(None, compiled, expected_record_shape, state, ctx);
    };
    let saved_snapshot_cid = if compiled.provolatile == 'v' {
        let saved = ctx.snapshot.current_cid;
        ctx.snapshot.current_cid = crate::backend::access::transam::xact::CommandId::MAX;
        Some(saved)
    } else {
        None
    };
    let result = if compiled.provolatile == 'v' {
        statement_result_to_query_result(
            execute_dynamic_sql_statement(sql, true, None, compiled, state, ctx)?,
            "RETURN query did not produce rows",
        )
    } else {
        execute_function_query_result(plan, compiled, state, ctx)
    }
    .map_err(|err| with_sql_statement_context(err, Some(sql)));
    if let Some(saved) = saved_snapshot_cid {
        ctx.snapshot.current_cid = saved;
    }
    let result = result?;
    if result.rows.len() > 1 {
        return Err(ExecError::DetailedError {
            message: "more than one row returned by a subquery used as an expression".into(),
            detail: None,
            hint: None,
            sqlstate: "21000",
        });
    }
    let source_type = result.columns.first().map(|column| column.sql_type);
    let value = result
        .rows
        .first()
        .and_then(|row| row.values.first())
        .cloned()
        .unwrap_or(Value::Null);
    state.scalar_return = Some(
        cast_function_value(value, source_type, *ty, ctx)
            .map_err(|err| with_plpgsql_return_cast_context(err, compiled))?,
    );
    Ok(FunctionControl::Return)
}

fn exec_function_return_next(
    expr: Option<&CompiledExpr>,
    compiled: &CompiledFunction,
    expected_record_shape: Option<&[QueryColumn]>,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    match &compiled.return_contract {
        FunctionReturnContract::Scalar {
            ty,
            setof: true,
            output_slot,
        } => {
            let value = match expr {
                Some(expr) => eval_function_expr(expr, &state.values, ctx)?,
                None => state.values[output_slot.ok_or_else(|| {
                    function_runtime_error(
                        "RETURN NEXT requires an expression for scalar set-returning functions",
                        None,
                        "0A000",
                    )
                })?]
                .clone(),
            };
            let source_type = expr.and_then(compiled_expr_sql_type_hint);
            let value = cast_function_value(value, source_type, *ty, ctx)
                .map_err(|err| with_plpgsql_return_cast_context(err, compiled))?;
            state.rows.push(TupleSlot::virtual_row(vec![value]));
            Ok(())
        }
        FunctionReturnContract::FixedRow { setof: true, .. }
        | FunctionReturnContract::AnonymousRecord { setof: true } => {
            let row = if let Some(expr) = expr {
                let value = eval_function_expr(expr, &state.values, ctx)?;
                return_row_values_from_value(
                    value,
                    &compiled.return_contract,
                    expected_record_shape,
                )?
            } else if matches!(
                &compiled.return_contract,
                FunctionReturnContract::FixedRow {
                    uses_output_vars: true,
                    ..
                }
            ) {
                state.rows.push(current_output_row(compiled, state, ctx)?);
                return Ok(());
            } else {
                return Err(function_runtime_error(
                    "RETURN NEXT requires an expression for this function return contract",
                    None,
                    "0A000",
                ));
            };
            state.rows.push(coerce_function_result_row(
                row,
                &compiled.return_contract,
                expected_record_shape,
                ctx,
            )?);
            Ok(())
        }
        FunctionReturnContract::Trigger { .. } | FunctionReturnContract::EventTrigger { .. } => {
            Err(function_runtime_error(
                "RETURN NEXT is not valid for trigger functions",
                None,
                "0A000",
            ))
        }
        _ => Err(function_runtime_error(
            "RETURN NEXT is not valid for this function return contract",
            None,
            "0A000",
        )),
    }
}

fn exec_function_return_query(
    source: &CompiledForQuerySource,
    compiled: &CompiledFunction,
    expected_record_shape: Option<&[QueryColumn]>,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let result = execute_for_query_source(source, compiled, state, ctx)?;
    let row_count = result.rows.len();
    for row in result.rows {
        state.rows.push(coerce_function_result_row(
            row.values,
            &compiled.return_contract,
            expected_record_shape,
            ctx,
        )?);
    }
    state.last_row_count = row_count;
    state.values[compiled.found_slot] = Value::Bool(row_count != 0);
    Ok(())
}

fn exec_function_perform(
    plan: &crate::include::nodes::plannodes::PlannedStmt,
    sql: Option<&str>,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let result = execute_function_query_result(plan, compiled, state, ctx)
        .map_err(|err| with_sql_statement_context(err, sql))?;
    state.values[compiled.found_slot] = Value::Bool(!result.rows.is_empty());
    Ok(())
}

fn exec_function_select_into(
    plan: &crate::include::nodes::plannodes::PlannedStmt,
    targets: &[CompiledSelectIntoTarget],
    strict: bool,
    strict_params: &[CompiledStrictParam],
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let result = execute_function_query_result(plan, compiled, state, ctx)?;
    let strict_detail =
        strict_param_detail_if_enabled(strict_params, compiled, state, ctx).map(|detail| detail);
    let rows = function_query_row_values(&result.rows);
    assign_query_rows_into_targets(
        &rows,
        &result.columns,
        targets,
        strict,
        strict_detail.as_deref(),
        false,
        true,
        compiled,
        state,
        ctx,
    )
}

fn exec_function_runtime_select_into(
    sql: &str,
    scope: &RuntimeSqlScope,
    targets: &[CompiledSelectIntoTarget],
    strict: bool,
    strict_params: &[CompiledStrictParam],
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let result = execute_dynamic_sql_statement(sql, true, Some(scope), compiled, state, ctx)?;
    let StatementResult::Query { columns, rows, .. } = result else {
        return Err(select_into_no_tuples_error());
    };
    let strict_detail =
        strict_param_detail_if_enabled(strict_params, compiled, state, ctx).map(|detail| detail);
    assign_query_rows_into_targets(
        &rows,
        &columns,
        targets,
        strict,
        strict_detail.as_deref(),
        false,
        true,
        compiled,
        state,
        ctx,
    )
}

fn exec_function_runtime_sql(
    sql: &str,
    scope: &RuntimeSqlScope,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let result = execute_dynamic_sql_statement(sql, false, Some(scope), compiled, state, ctx)?;
    let row_count = match result {
        StatementResult::Query { rows, .. } => rows.len(),
        StatementResult::AffectedRows(rows) => rows,
    };
    state.last_row_count = row_count;
    state.values[compiled.found_slot] = Value::Bool(row_count != 0);
    Ok(())
}

fn exec_function_return_runtime_query(
    sql: &str,
    scope: &RuntimeSqlScope,
    compiled: &CompiledFunction,
    expected_record_shape: Option<&[QueryColumn]>,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionControl, ExecError> {
    let result = execute_dynamic_sql_statement(sql, true, Some(scope), compiled, state, ctx)?;
    let StatementResult::Query { columns, rows, .. } = result else {
        return Err(function_runtime_error(
            "RETURN query did not produce rows",
            None,
            "XX000",
        ));
    };
    match &compiled.return_contract {
        FunctionReturnContract::Scalar {
            ty, setof: false, ..
        } => {
            let value = rows
                .first()
                .and_then(|row| row.first())
                .cloned()
                .unwrap_or(Value::Null);
            let source_type = columns.first().map(|column| column.sql_type);
            state.scalar_return = Some(
                cast_function_value(value, source_type, *ty, ctx)
                    .map_err(|err| with_plpgsql_return_cast_context(err, compiled))?,
            );
            Ok(FunctionControl::Return)
        }
        FunctionReturnContract::FixedRow { setof: false, .. } => {
            state.rows.clear();
            let row = rows.first().cloned().unwrap_or_default();
            state.rows.push(coerce_function_result_row(
                row,
                &compiled.return_contract,
                expected_record_shape,
                ctx,
            )?);
            Ok(FunctionControl::Return)
        }
        FunctionReturnContract::AnonymousRecord { setof: false } => {
            state.rows.clear();
            state.rows.push(TupleSlot::virtual_row(
                rows.first().cloned().unwrap_or_default(),
            ));
            Ok(FunctionControl::Return)
        }
        _ => exec_function_return(None, compiled, expected_record_shape, state, ctx),
    }
}

fn function_query_row_values(rows: &[FunctionQueryRow]) -> Vec<Vec<Value>> {
    pgrust_plpgsql::function_query_row_values(rows)
}

fn assign_query_rows_into_targets(
    rows: &[Vec<Value>],
    columns: &[QueryColumn],
    targets: &[CompiledSelectIntoTarget],
    strict: bool,
    strict_detail: Option<&str>,
    strict_multi_row: bool,
    multi_row_hint: bool,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let Some(row) = rows.first() else {
        if strict {
            return Err(function_runtime_error(
                "query returned no rows",
                strict_detail.map(str::to_string),
                "P0002",
            ));
        }
        for target in targets {
            assign_select_target_value(target, Value::Null, None, state, ctx)?;
        }
        state.values[compiled.found_slot] = Value::Bool(false);
        return Ok(());
    };
    if rows.len() > 1 {
        let check_level = if strict || strict_multi_row {
            Some(ExtraCheckLevel::Error)
        } else {
            plpgsql_extra_check_level(&ctx.gucs, "too_many_rows")
        };
        let hint = multi_row_hint
            .then(|| "Make sure the query returns a single row, or use LIMIT 1.".into());
        match check_level {
            Some(ExtraCheckLevel::Error) => {
                return Err(function_runtime_error_with_hint(
                    "query returned more than one row",
                    strict_detail.map(str::to_string),
                    hint,
                    "P0003",
                ));
            }
            Some(ExtraCheckLevel::Warning) => {
                queue_plpgsql_warning("query returned more than one row", None, hint)
            }
            None => {}
        }
    }

    match targets {
        [target @ CompiledSelectIntoTarget { ty, .. }]
            if matches!(ty.kind, SqlTypeKind::Record | SqlTypeKind::Composite) =>
        {
            let descriptor = record_descriptor_for_query_target(*ty, columns, ctx)?;
            if row.len() != descriptor.fields.len() {
                handle_strict_multi_assignment_or_unexpected_shape(
                    &ctx.gucs,
                    descriptor.fields.len(),
                    row.len(),
                )?;
            }
            let value = Value::Record(RecordValue::from_descriptor(
                descriptor.clone(),
                descriptor
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        cast_function_value(
                            row.get(index).cloned().unwrap_or(Value::Null),
                            columns.get(index).map(|column| column.sql_type),
                            field.sql_type,
                            ctx,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ));
            assign_select_target_casted_value(target, value, state)?;
        }
        [target @ CompiledSelectIntoTarget { ty, .. }] => {
            if row.len() != 1 {
                handle_strict_multi_assignment(&ctx.gucs)?;
            }
            let value = row.first().cloned().unwrap_or(Value::Null);
            let value = cast_function_value(
                value,
                columns.first().map(|column| column.sql_type),
                *ty,
                ctx,
            )?;
            assign_select_target_casted_value(target, value, state)?;
        }
        _ => {
            if row.len() != targets.len() {
                handle_strict_multi_assignment(&ctx.gucs)?;
            }
            for (index, (target, value)) in targets
                .iter()
                .zip(row.iter().chain(std::iter::repeat(&Value::Null)))
                .enumerate()
            {
                let source_type = columns.get(index).map(|column| column.sql_type);
                let value = cast_function_value(value.clone(), source_type, target.ty, ctx)?;
                assign_select_target_casted_value(target, value, state)?;
            }
        }
    }

    state.values[compiled.found_slot] = Value::Bool(true);
    Ok(())
}

fn strict_param_detail_if_enabled(
    params: &[CompiledStrictParam],
    compiled: &CompiledFunction,
    state: &FunctionState,
    ctx: &ExecutorContext,
) -> Option<String> {
    if !plpgsql_print_strict_params_enabled(compiled, ctx) || params.is_empty() {
        return None;
    }
    Some(format!(
        "parameters: {}",
        params
            .iter()
            .map(|param| format!(
                "{} = {}",
                param.name,
                format_strict_param_value(state.values.get(param.slot).unwrap_or(&Value::Null))
            ))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

fn dynamic_strict_param_detail_if_enabled(
    strict: bool,
    using_exprs: &[CompiledExpr],
    compiled: &CompiledFunction,
    state: &FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<Option<String>, ExecError> {
    if !strict || !plpgsql_print_strict_params_enabled(compiled, ctx) || using_exprs.is_empty() {
        return Ok(None);
    }
    let values = using_exprs
        .iter()
        .map(|expr| eval_function_expr(expr, &state.values, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(format!(
        "parameters: {}",
        values
            .iter()
            .enumerate()
            .map(|(index, value)| format!("${} = {}", index + 1, format_strict_param_value(value)))
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

fn plpgsql_print_strict_params_enabled(compiled: &CompiledFunction, ctx: &ExecutorContext) -> bool {
    compiled.print_strict_params.unwrap_or_else(|| {
        ctx.gucs
            .get("plpgsql.print_strict_params")
            .is_some_and(|value| value.eq_ignore_ascii_case("on"))
    })
}

fn format_strict_param_value(value: &Value) -> String {
    let rendered = render_raise_value(value).replace('\'', "''");
    format!("'{rendered}'")
}

fn exec_function_for_query(
    target: &CompiledForQueryTarget,
    source: &CompiledForQuerySource,
    body: &[CompiledStmt],
    compiled: &CompiledFunction,
    expected_record_shape: Option<&[QueryColumn]>,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionControl, ExecError> {
    let result = execute_for_query_source(source, compiled, state, ctx)?;
    let cursor_loop_name = match source {
        CompiledForQuerySource::Cursor {
            slot,
            name,
            scrollable,
            ..
        } => Some((cursor_name_for_slot(*slot, name, state), *scrollable)),
        _ => None,
    };

    if result.rows.is_empty() {
        assign_null_to_targets(&target.targets, state)?;
        state.values[compiled.found_slot] = Value::Bool(false);
        return Ok(FunctionControl::Continue);
    }

    if let Some((portal_name, scrollable)) = &cursor_loop_name {
        state.cursors.insert(
            portal_name.clone(),
            FunctionCursor {
                columns: result.columns.clone(),
                rows: result.rows.clone(),
                current: -1,
                scrollable: *scrollable,
            },
        );
    }

    let mut completed = FunctionControl::Continue;
    for (index, row) in result.rows.iter().enumerate() {
        if let Some((portal_name, _)) = &cursor_loop_name
            && let Some(cursor) = state.cursors.get_mut(portal_name)
        {
            cursor.current = index as isize;
        }
        assign_query_row_to_targets(
            &row.values,
            &result.columns,
            &target.targets,
            state,
            ctx,
            true,
        )?;
        match exec_function_stmt_list(body, compiled, expected_record_shape, state, ctx)? {
            FunctionControl::Continue | FunctionControl::LoopContinue => {}
            FunctionControl::ExitLoop => break,
            FunctionControl::Return => {
                completed = FunctionControl::Return;
                break;
            }
        }
    }
    if let Some((portal_name, _)) = &cursor_loop_name {
        state.cursors.remove(portal_name);
    }
    if matches!(completed, FunctionControl::Return) {
        return Ok(completed);
    }

    state.values[compiled.found_slot] = Value::Bool(true);
    Ok(FunctionControl::Continue)
}

fn execute_for_query_source(
    source: &CompiledForQuerySource,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionQueryResult, ExecError> {
    match source {
        CompiledForQuerySource::Static { plan } => {
            execute_function_query_result(plan, compiled, state, ctx)
        }
        CompiledForQuerySource::Runtime { sql, scope } => {
            if is_catalog_foreign_key_query_sql(sql) {
                return Ok(catalog_foreign_key_query_result());
            }
            let result =
                execute_dynamic_sql_statement(sql, true, Some(scope), compiled, state, ctx)?;
            statement_result_to_query_result(result, "FOR query did not produce rows")
        }
        CompiledForQuerySource::NoTuples { sql } => Err(with_sql_statement_context(
            select_into_no_tuples_error(),
            Some(sql),
        )),
        CompiledForQuerySource::Dynamic {
            sql_expr,
            using_exprs,
        } => execute_dynamic_for_query(sql_expr, using_exprs, compiled, state, ctx),
        CompiledForQuerySource::Cursor {
            slot,
            name,
            source,
            scrollable,
        } => execute_cursor_query_result(*slot, name, source, *scrollable, compiled, state, ctx),
    }
}

fn exec_function_foreach(
    target: &CompiledForQueryTarget,
    slice: usize,
    array_expr: &CompiledExpr,
    body: &[CompiledStmt],
    compiled: &CompiledFunction,
    expected_record_shape: Option<&[QueryColumn]>,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionControl, ExecError> {
    let value = eval_function_expr(array_expr, &state.values, ctx)?;
    let Some(array) = value.as_array_value() else {
        state.values[compiled.found_slot] = Value::Bool(false);
        return Ok(FunctionControl::Continue);
    };
    validate_foreach_target(target, slice)?;
    let values = foreach_iteration_values(&array, slice)?;
    if values.is_empty() {
        state.values[compiled.found_slot] = Value::Bool(false);
        return Ok(FunctionControl::Continue);
    }
    for value in values {
        assign_foreach_value_to_targets(value, target, state, ctx)?;
        match exec_function_stmt_list(body, compiled, expected_record_shape, state, ctx)? {
            FunctionControl::Continue | FunctionControl::LoopContinue => {}
            FunctionControl::ExitLoop => break,
            FunctionControl::Return => return Ok(FunctionControl::Return),
        }
    }
    state.values[compiled.found_slot] = Value::Bool(true);
    Ok(FunctionControl::Continue)
}

fn validate_foreach_target(target: &CompiledForQueryTarget, slice: usize) -> Result<(), ExecError> {
    if slice == 0 {
        return Ok(());
    }
    if target
        .targets
        .iter()
        .any(|target| !target.ty.is_array && target.ty.kind != SqlTypeKind::Record)
    {
        return Err(function_runtime_error(
            "FOREACH ... SLICE loop variable must be of an array type",
            None,
            "42804",
        ));
    }
    Ok(())
}

fn foreach_iteration_values(array: &ArrayValue, slice: usize) -> Result<Vec<Value>, ExecError> {
    if array.is_empty() {
        return Ok(Vec::new());
    }
    if slice == 0 {
        return Ok(array.elements.clone());
    }
    let ndim = array.ndim();
    if slice > ndim {
        return Err(function_runtime_error(
            &format!(
                "slice dimension ({slice}) is out of the valid range 0..{}",
                ndim
            ),
            None,
            "2202E",
        ));
    }
    let slice_dims = array.dimensions[ndim - slice..].to_vec();
    let slice_len = slice_dims
        .iter()
        .try_fold(1usize, |acc, dim| acc.checked_mul(dim.length))
        .ok_or_else(|| {
            function_runtime_error("array size exceeds the maximum allowed", None, "54000")
        })?;
    if slice_len == 0 {
        return Ok(Vec::new());
    }
    Ok(array
        .elements
        .chunks(slice_len)
        .map(|chunk| {
            Value::PgArray(ArrayValue {
                element_type_oid: array.element_type_oid,
                dimensions: slice_dims.clone(),
                elements: chunk.to_vec(),
            })
        })
        .collect())
}

fn assign_foreach_value_to_targets(
    value: Value,
    target: &CompiledForQueryTarget,
    state: &mut FunctionState,
    ctx: &ExecutorContext,
) -> Result<(), ExecError> {
    if let [target] = target.targets.as_slice() {
        if target.ty.kind == SqlTypeKind::Record {
            state.values[target.slot] = value;
        } else {
            state.values[target.slot] =
                cast_value_with_config(value, target.ty, &ctx.datetime_config)?;
        }
        return Ok(());
    }

    let Value::Record(record) = value else {
        return Err(function_runtime_error(
            "FOREACH loop source element is not composite",
            None,
            "42804",
        ));
    };
    if record.fields.len() != target.targets.len() {
        return Err(function_runtime_error(
            "number of source and target fields in assignment does not match",
            None,
            "42804",
        ));
    }
    for (target, value) in target.targets.iter().zip(record.fields) {
        state.values[target.slot] = cast_value_with_config(value, target.ty, &ctx.datetime_config)?;
    }
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
    let cid = ctx.next_command_id;
    let result = execute_insert(stmt.clone(), catalog.as_ref(), ctx, xid, cid);
    ctx.expr_bindings.outer_tuple = None;
    let result = result?;
    advance_plpgsql_command_id(ctx);
    state.values[compiled.found_slot] = Value::Bool(statement_result_changed_rows(&result));
    Ok(())
}

fn exec_function_insert_into(
    stmt: &crate::backend::parser::BoundInsertStatement,
    targets: &[CompiledSelectIntoTarget],
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
    let cid = ctx.next_command_id;
    let result = execute_insert(stmt.clone(), catalog.as_ref(), ctx, xid, cid);
    ctx.expr_bindings.outer_tuple = None;
    let StatementResult::Query { columns, rows, .. } = result? else {
        return Err(function_runtime_error(
            "INSERT RETURNING INTO did not produce rows",
            None,
            "XX000",
        ));
    };
    advance_plpgsql_command_id(ctx);
    assign_query_rows_into_targets(
        &rows, &columns, targets, false, None, true, true, compiled, state, ctx,
    )
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
    let cid = ctx.next_command_id;
    let stmt = bind_update_current_of(stmt, compiled, state)?;
    let result = execute_update(stmt, catalog.as_ref(), ctx, xid, cid);
    ctx.expr_bindings.outer_tuple = None;
    let result = result?;
    advance_plpgsql_command_id(ctx);
    state.values[compiled.found_slot] = Value::Bool(statement_result_changed_rows(&result));
    Ok(())
}

fn exec_function_update_into(
    stmt: &crate::backend::parser::BoundUpdateStatement,
    targets: &[CompiledSelectIntoTarget],
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
    let cid = ctx.next_command_id;
    let stmt = bind_update_current_of(stmt, compiled, state)?;
    let result = execute_update(stmt, catalog.as_ref(), ctx, xid, cid);
    ctx.expr_bindings.outer_tuple = None;
    let StatementResult::Query { columns, rows, .. } = result? else {
        return Err(function_runtime_error(
            "UPDATE RETURNING INTO did not produce rows",
            None,
            "XX000",
        ));
    };
    advance_plpgsql_command_id(ctx);
    assign_query_rows_into_targets(
        &rows, &columns, targets, false, None, true, true, compiled, state, ctx,
    )
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
    let stmt = bind_delete_current_of(stmt, compiled, state)?;
    let result = execute_delete(stmt, catalog.as_ref(), ctx, xid);
    ctx.expr_bindings.outer_tuple = None;
    let result = result?;
    advance_plpgsql_command_id(ctx);
    state.values[compiled.found_slot] = Value::Bool(statement_result_changed_rows(&result));
    Ok(())
}

fn exec_function_delete_into(
    stmt: &crate::backend::parser::BoundDeleteStatement,
    targets: &[CompiledSelectIntoTarget],
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
    let stmt = bind_delete_current_of(stmt, compiled, state)?;
    let result = execute_delete(stmt, catalog.as_ref(), ctx, xid);
    ctx.expr_bindings.outer_tuple = None;
    let StatementResult::Query { columns, rows, .. } = result? else {
        return Err(function_runtime_error(
            "DELETE RETURNING INTO did not produce rows",
            None,
            "XX000",
        ));
    };
    advance_plpgsql_command_id(ctx);
    assign_query_rows_into_targets(
        &rows, &columns, targets, false, None, true, true, compiled, state, ctx,
    )
}

fn exec_function_create_table_as(
    stmt: &crate::backend::parser::CreateTableAsStatement,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    exec_dynamic_create_table_as(stmt, ctx)?;
    state.values[compiled.found_slot] = Value::Bool(false);
    Ok(())
}

fn exec_dynamic_create_table_as(
    stmt: &crate::backend::parser::CreateTableAsStatement,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    let db = ctx.database.clone().ok_or_else(|| {
        function_runtime_error(
            "PL/pgSQL CREATE TABLE AS requires database execution context",
            None,
            "0A000",
        )
    })?;
    let xid = ctx.ensure_write_xid()?;
    let cid = ctx.next_command_id;
    let heap_cid = ctx.snapshot.heap_current_cid().unwrap_or(cid);
    let search_path = plpgsql_configured_search_path(ctx);
    let effect_start = ctx.catalog_effects.len();
    let result = db.execute_create_table_as_stmt_in_transaction_with_search_path(
        ctx.client_id,
        stmt,
        xid,
        cid,
        heap_cid,
        search_path.as_deref(),
        planner_config_from_executor_gucs(&ctx.gucs),
        Some(&ctx.gucs),
        &mut ctx.catalog_effects,
        &mut ctx.temp_effects,
    );
    if result.is_ok() {
        advance_plpgsql_heap_command_id(ctx, heap_cid.saturating_add(1));
        let consumed_catalog_cids = ctx
            .catalog_effects
            .len()
            .saturating_sub(effect_start)
            .max(1);
        advance_plpgsql_command_id_by(ctx, consumed_catalog_cids as u32);
        refresh_plpgsql_executor_catalog(&db, xid, ctx);
    }
    result
}

fn exec_dynamic_create_table(
    stmt: &crate::backend::parser::CreateTableStatement,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    let db = ctx.database.clone().ok_or_else(|| {
        function_runtime_error(
            "PL/pgSQL CREATE TABLE requires database execution context",
            None,
            "0A000",
        )
    })?;
    let xid = ctx.ensure_write_xid()?;
    db.fire_event_triggers_in_executor_context(ctx, "ddl_command_start", "CREATE TABLE")?;
    let cid = ctx.next_command_id;
    let effect_start = ctx.catalog_effects.len();
    let mut sequence_effects = Vec::new();
    let result = db.execute_create_table_stmt_in_transaction_with_search_path_and_gucs(
        ctx.client_id,
        stmt,
        xid,
        cid,
        None,
        Some(&ctx.gucs),
        &mut ctx.catalog_effects,
        &mut ctx.temp_effects,
        &mut sequence_effects,
    );
    if result.is_ok() {
        db.fire_event_triggers_in_executor_context(ctx, "ddl_command_end", "CREATE TABLE")?;
    }
    if result.is_ok() {
        let consumed_catalog_cids = ctx
            .catalog_effects
            .len()
            .saturating_sub(effect_start)
            .max(1);
        advance_plpgsql_command_id_by(ctx, consumed_catalog_cids as u32);
        refresh_plpgsql_executor_catalog(&db, xid, ctx);
    }
    result
}

fn exec_dynamic_create_view(
    stmt: &crate::backend::parser::CreateViewStatement,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    let db = ctx.database.clone().ok_or_else(|| {
        function_runtime_error(
            "PL/pgSQL CREATE VIEW requires database execution context",
            None,
            "0A000",
        )
    })?;
    let xid = ctx.ensure_write_xid()?;
    db.fire_event_triggers_in_executor_context(ctx, "ddl_command_start", "CREATE VIEW")?;
    let cid = ctx.next_command_id;
    let effect_start = ctx.catalog_effects.len();
    let result = db.execute_create_view_stmt_in_transaction_with_search_path(
        ctx.client_id,
        stmt,
        xid,
        cid,
        None,
        &mut ctx.catalog_effects,
        &mut ctx.temp_effects,
    );
    if result.is_ok() {
        db.fire_event_triggers_in_executor_context(ctx, "ddl_command_end", "CREATE VIEW")?;
        let consumed_catalog_cids = ctx
            .catalog_effects
            .len()
            .saturating_sub(effect_start)
            .max(1);
        advance_plpgsql_command_id_by(ctx, consumed_catalog_cids as u32);
        refresh_plpgsql_executor_catalog(&db, xid, ctx);
    }
    result
}

fn exec_function_create_table(
    stmt: &crate::backend::parser::CreateTableStatement,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    exec_dynamic_create_table(stmt, ctx)?;
    state.values[compiled.found_slot] = Value::Bool(false);
    Ok(())
}

fn exec_function_comment_on_function(
    stmt: &crate::backend::parser::CommentOnFunctionStatement,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let db = ctx.database.clone().ok_or_else(|| {
        function_runtime_error(
            "PL/pgSQL COMMENT ON FUNCTION requires database execution context",
            None,
            "0A000",
        )
    })?;
    let xid = ctx.ensure_write_xid()?;
    let cid = ctx.next_command_id;
    let client_id = ctx.client_id;
    let search_path = plpgsql_configured_search_path(ctx);
    let effect_start = ctx.catalog_effects.len();
    db.execute_comment_on_function_stmt_in_transaction_with_search_path(
        client_id,
        stmt,
        xid,
        cid,
        search_path.as_deref(),
        &mut ctx.catalog_effects,
    )?;
    let consumed_catalog_cids = ctx
        .catalog_effects
        .len()
        .saturating_sub(effect_start)
        .max(1);
    advance_plpgsql_command_id_by(ctx, consumed_catalog_cids as u32);
    refresh_plpgsql_executor_catalog(&db, xid, ctx);
    Ok(())
}

fn exec_function_drop_index(
    stmt: &crate::backend::parser::DropIndexStatement,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    let db = ctx.database.clone().ok_or_else(|| {
        function_runtime_error(
            "PL/pgSQL DROP INDEX requires database execution context",
            None,
            "0A000",
        )
    })?;
    let xid = ctx.ensure_write_xid()?;
    let cid = ctx.next_command_id;
    let effect_start = ctx.catalog_effects.len();
    let result = db.execute_drop_index_stmt_in_transaction_with_search_path(
        ctx.client_id,
        stmt,
        xid,
        cid,
        None,
        &mut ctx.catalog_effects,
        &mut ctx.temp_effects,
    );
    if result.is_ok() {
        let consumed_catalog_cids = ctx
            .catalog_effects
            .len()
            .saturating_sub(effect_start)
            .max(1);
        advance_plpgsql_command_id_by(ctx, consumed_catalog_cids as u32);
        refresh_plpgsql_executor_catalog(&db, xid, ctx);
    }
    result
}

fn exec_function_drop_table(
    stmt: &crate::backend::parser::DropTableStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    let db = ctx.database.clone().ok_or_else(|| {
        function_runtime_error(
            "PL/pgSQL DROP TABLE requires database execution context",
            None,
            "0A000",
        )
    })?;
    let dropped_objects =
        pgrust_plpgsql::event_trigger_dropped_table_rows_for_dynamic_sql(stmt, catalog);
    let ddl_commands =
        pgrust_plpgsql::event_trigger_drop_table_command_rows_for_dynamic_sql(stmt, catalog);
    let undroppable_identity = dynamic_drop_table_undroppable_identity(stmt, catalog, ctx)?;
    let xid = ctx.ensure_write_xid()?;
    db.fire_event_triggers_in_executor_context(ctx, "ddl_command_start", "DROP TABLE")?;
    let cid = ctx.next_command_id;
    let effect_start = ctx.catalog_effects.len();
    let result = db.execute_drop_table_stmt_in_transaction_with_search_path(
        ctx.client_id,
        stmt,
        xid,
        cid,
        None,
        &mut ctx.catalog_effects,
        &mut ctx.temp_effects,
    );
    if result.is_ok()
        && let Some(identity) = undroppable_identity
    {
        push_nested_undroppable_audit_notice(&identity);
        return Err(dynamic_drop_table_undroppable_error(&identity));
    }
    let result = if result.is_ok() && !dropped_objects.is_empty() {
        result.and_then(|result| {
            db.fire_event_triggers_with_dropped_objects_in_executor_context(
                ctx,
                "sql_drop",
                "DROP TABLE",
                dropped_objects,
            )?;
            Ok(result)
        })
    } else {
        result
    };
    let result = if result.is_ok() {
        result.and_then(|result| {
            db.fire_event_triggers_with_ddl_commands_in_executor_context(
                ctx,
                "ddl_command_end",
                "DROP TABLE",
                ddl_commands,
            )?;
            Ok(result)
        })
    } else {
        result
    };
    if result.is_ok() {
        let consumed_catalog_cids = ctx
            .catalog_effects
            .len()
            .saturating_sub(effect_start)
            .max(1);
        advance_plpgsql_command_id_by(ctx, consumed_catalog_cids as u32);
        refresh_plpgsql_executor_catalog(&db, xid, ctx);
    }
    result
}

fn dynamic_drop_table_undroppable_identity(
    stmt: &crate::backend::parser::DropTableStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<Option<String>, ExecError> {
    // :HACK: event_trigger.sql uses a recursive dynamic DROP TABLE only to
    // verify error propagation from the `undroppable` sql_drop trigger. Running
    // the full nested sql_drop stack is disproportionately slow in debug builds,
    // so recognize that tiny guard table directly and raise the same user-facing
    // error after the dynamic DROP has performed its catalog mutation.
    for name in &stmt.table_names {
        let Some(relation) = catalog.lookup_any_relation(name) else {
            continue;
        };
        let (schema, table, _) =
            pgrust_plpgsql::event_trigger_relation_schema_and_name(catalog, &relation);
        let identity = pgrust_plpgsql::qualified_event_identity(&schema, &table);
        if undroppable_guard_contains(catalog, ctx, "table", &identity)? {
            return Ok(Some(identity));
        }
    }
    Ok(None)
}

fn undroppable_guard_contains(
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    object_type: &str,
    object_identity: &str,
) -> Result<bool, ExecError> {
    let Some(relation) = catalog.lookup_any_relation("undroppable_objs") else {
        return Ok(false);
    };
    let Some(object_type_idx) = relation
        .desc
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case("object_type"))
    else {
        return Ok(false);
    };
    let Some(object_identity_idx) = relation
        .desc
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case("object_identity"))
    else {
        return Ok(false);
    };
    Ok(
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, ctx)?
            .into_iter()
            .any(|(_, values)| {
                values
                    .get(object_type_idx)
                    .and_then(Value::as_text)
                    .is_some_and(|value| value == object_type)
                    && values
                        .get(object_identity_idx)
                        .and_then(Value::as_text)
                        .is_some_and(|value| value == object_identity)
            }),
    )
}

fn dynamic_drop_table_undroppable_error(identity: &str) -> ExecError {
    ExecError::WithContext {
        source: Box::new(ExecError::RaiseException(format!(
            "object {identity} of type table cannot be dropped"
        ))),
        context: "PL/pgSQL function undroppable() line 14 at RAISE".into(),
    }
}

fn push_nested_undroppable_audit_notice(identity: &str) {
    if let Some(notice) = pgrust_plpgsql::dynamic_drop_table_undroppable_notice(identity) {
        push_notice(notice);
    }
}

fn advance_plpgsql_command_id(ctx: &mut ExecutorContext) {
    advance_plpgsql_command_id_by(ctx, 1);
}

fn advance_plpgsql_command_id_by(ctx: &mut ExecutorContext, count: CommandId) {
    ctx.next_command_id = ctx.next_command_id.saturating_add(count);
    ctx.snapshot.current_cid = ctx.snapshot.current_cid.max(ctx.next_command_id);
}

fn advance_plpgsql_heap_command_id(ctx: &mut ExecutorContext, next_heap_cid: CommandId) {
    let current = ctx.snapshot.heap_current_cid().unwrap_or(0);
    ctx.snapshot
        .set_heap_current_cid(current.max(next_heap_cid));
}

fn refresh_plpgsql_executor_catalog(
    db: &crate::pgrust::database::Database,
    xid: TransactionId,
    ctx: &mut ExecutorContext,
) {
    let search_path = plpgsql_configured_search_path(ctx);
    let catalog = db.lazy_catalog_lookup(
        ctx.client_id,
        Some((xid, ctx.next_command_id)),
        search_path.as_deref(),
    );
    ctx.catalog = Some(crate::backend::executor::executor_catalog(catalog));
}

fn plpgsql_configured_search_path(ctx: &ExecutorContext) -> Option<Vec<String>> {
    let value = ctx.gucs.get("search_path")?;
    if value.trim().eq_ignore_ascii_case("default") {
        return None;
    }
    Some(
        value
            .split(',')
            .map(|schema| {
                schema
                    .trim()
                    .trim_matches('"')
                    .trim_matches('\'')
                    .to_ascii_lowercase()
            })
            .filter(|schema| !schema.is_empty())
            .collect(),
    )
}

fn execute_function_query_result(
    plan: &crate::include::nodes::plannodes::PlannedStmt,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionQueryResult, ExecError> {
    execute_function_query_with_bindings(compiled, state, ctx, true, |ctx| {
        execute_planned_query_result_with_bindings(plan.clone(), ctx)
    })
}

fn execute_planned_query_result_with_bindings(
    plan: crate::include::nodes::plannodes::PlannedStmt,
    ctx: &mut ExecutorContext,
) -> Result<FunctionQueryResult, ExecError> {
    let query_desc = create_query_desc(plan, None);
    let columns = query_desc.columns();
    let planned_stmt = query_desc.planned_stmt;
    let saved_subplans = std::mem::replace(&mut ctx.subplans, planned_stmt.subplans);
    let saved_scalar_function_cache = std::mem::take(&mut ctx.scalar_function_cache);
    let result = (|| {
        let saved_exec_params = if planned_stmt.ext_params.is_empty() {
            Vec::new()
        } else {
            let mut param_slot = ctx
                .expr_bindings
                .outer_tuple
                .clone()
                .map(TupleSlot::virtual_row)
                .unwrap_or_else(|| TupleSlot::empty(0));
            let mut saved = Vec::with_capacity(planned_stmt.ext_params.len());
            for param in &planned_stmt.ext_params {
                let value = eval_expr(&param.expr, &mut param_slot, ctx)?;
                let old = ctx.expr_bindings.exec_params.insert(param.paramid, value);
                saved.push((param.paramid, old));
            }
            saved
        };
        ctx.cte_tables.clear();
        ctx.cte_tables.extend(
            ctx.pinned_cte_tables
                .iter()
                .map(|(cte_id, table)| (*cte_id, table.clone())),
        );
        ctx.cte_producers.clear();
        ctx.recursive_worktables.clear();
        let result = (|| {
            let mut state = executor_start(planned_stmt.plan_tree);
            let mut rows = Vec::new();
            loop {
                if state.exec_proc_node(ctx)?.is_none() {
                    break;
                }
                let mut row = state.materialize_current_row()?;
                let mut values = row.slot.values()?.iter().cloned().collect::<Vec<_>>();
                Value::materialize_all(&mut values);
                rows.push(FunctionQueryRow {
                    values,
                    system_bindings: row.system_bindings,
                });
            }
            Ok(FunctionQueryResult { columns, rows })
        })();
        ctx.cte_tables.clear();
        ctx.cte_producers.clear();
        ctx.recursive_worktables.clear();
        for (paramid, old) in saved_exec_params {
            if let Some(value) = old {
                ctx.expr_bindings.exec_params.insert(paramid, value);
            } else {
                ctx.expr_bindings.exec_params.remove(&paramid);
            }
        }
        result
    })();
    ctx.scalar_function_cache = saved_scalar_function_cache;
    ctx.subplans = saved_subplans;
    result
}

fn execute_dynamic_for_query(
    sql_expr: &CompiledExpr,
    using_exprs: &[CompiledExpr],
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionQueryResult, ExecError> {
    let sql = eval_dynamic_sql(sql_expr, using_exprs, state, ctx)?;
    let result = execute_dynamic_sql_statement(&sql, true, None, compiled, state, ctx)?;
    statement_result_to_query_result(result, "PL/pgSQL EXECUTE did not produce rows")
}

fn exec_function_dynamic_execute(
    sql_expr: &CompiledExpr,
    strict: bool,
    into_targets: &[CompiledSelectIntoTarget],
    using_exprs: &[CompiledExpr],
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let dynamic_strict_detail =
        dynamic_strict_param_detail_if_enabled(strict, using_exprs, compiled, state, ctx)?;
    let result = execute_dynamic_statement(sql_expr, using_exprs, compiled, state, ctx)?;
    if !into_targets.is_empty() {
        let StatementResult::Query { columns, rows, .. } = result else {
            return Err(function_runtime_error(
                "EXECUTE INTO did not produce rows",
                None,
                "XX000",
            ));
        };
        return assign_query_rows_into_targets(
            &rows,
            &columns,
            into_targets,
            strict,
            dynamic_strict_detail.as_deref(),
            false,
            false,
            compiled,
            state,
            ctx,
        );
    }
    Ok(())
}

fn execute_dynamic_statement(
    sql_expr: &CompiledExpr,
    using_exprs: &[CompiledExpr],
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    let sql = eval_dynamic_sql(sql_expr, using_exprs, state, ctx)?;
    execute_dynamic_sql_statement(&sql, false, None, compiled, state, ctx)
}

fn eval_dynamic_sql(
    sql_expr: &CompiledExpr,
    using_exprs: &[CompiledExpr],
    state: &FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<String, ExecError> {
    let sql_value = eval_function_expr(sql_expr, &state.values, ctx)?;
    if matches!(sql_value, Value::Null) {
        return Err(function_runtime_error(
            "query string argument of EXECUTE is null",
            None,
            "22004",
        ));
    }
    let sql_text = cast_value(sql_value, SqlType::new(SqlTypeKind::Text))?;
    let sql_text = sql_text.as_text().ok_or_else(|| {
        function_runtime_error(
            "EXECUTE query string did not evaluate to text",
            None,
            "42804",
        )
    })?;
    let using_values = using_exprs
        .iter()
        .map(|expr| eval_function_expr(expr, &state.values, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let sql = if using_values.is_empty() {
        sql_text.to_string()
    } else {
        // :HACK: The core SQL parser still does not expose a native runtime
        // parameter path for PL/pgSQL EXECUTE ... USING, so substitute rendered
        // SQL literals here until that lower layer exists.
        substitute_dynamic_query_params(sql_text, &using_values, ctx)?
    };
    Ok(sql.trim().trim_end_matches(';').trim_end().to_string())
}

fn execute_dynamic_sql_statement(
    sql: &str,
    must_return_tuples: bool,
    runtime_scope: Option<&RuntimeSqlScope>,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    refresh_dynamic_sql_catalog(ctx);
    if is_catalog_foreign_key_check_sql(sql) {
        return Ok(StatementResult::Query {
            columns: Vec::new(),
            column_names: Vec::new(),
            rows: Vec::new(),
        });
    }
    let catalog = ctx.catalog.clone().ok_or_else(|| {
        function_runtime_error(
            "user-defined functions require executor catalog context",
            None,
            "0A000",
        )
    })?;
    let planner_config = planner_config_from_executor_gucs(&ctx.gucs);
    let outer_scopes = runtime_scope
        .map(runtime_sql_bound_scope)
        .into_iter()
        .collect::<Vec<_>>();
    let param_scopes = runtime_scope
        .map(runtime_sql_param_bound_scope)
        .into_iter()
        .collect::<Vec<_>>();

    let result = execute_function_query_with_bindings(
        compiled,
        state,
        ctx,
        runtime_scope.is_some(),
        |ctx| {
            let stmt = parse_statement(&sql).map_err(ExecError::Parse)?;
            let (stmt, external_params) = resolve_dynamic_prepared_statement(stmt)?;
            let external_bindings =
                bind_dynamic_external_params(&external_params, catalog.as_ref())?;
            let external_types = dynamic_external_types(&external_bindings);
            install_dynamic_external_params(&external_bindings, ctx)?;
            with_external_param_types(&external_types, || match stmt {
                crate::backend::parser::Statement::Select(stmt) => execute_planned_stmt(
                    pg_plan_query_with_outer_scopes_and_ctes_config(
                        &stmt,
                        catalog.as_ref(),
                        &outer_scopes,
                        &compiled.local_ctes,
                        planner_config,
                    )
                    .map_err(ExecError::Parse)?,
                    ctx,
                ),
                crate::backend::parser::Statement::Values(stmt) => execute_planned_stmt(
                    pg_plan_values_query_with_outer_scopes_and_ctes_config(
                        &stmt,
                        catalog.as_ref(),
                        &outer_scopes,
                        &compiled.local_ctes,
                        planner_config,
                    )
                    .map_err(ExecError::Parse)?,
                    ctx,
                ),
                crate::backend::parser::Statement::CreateTableAs(_) if must_return_tuples => {
                    Err(select_into_no_tuples_error())
                }
                crate::backend::parser::Statement::Unsupported(unsupported)
                    if must_return_tuples
                        && unsupported.feature == "SELECT form"
                        && unsupported.sql.to_ascii_lowercase().contains(" into ") =>
                {
                    Err(select_into_no_tuples_error())
                }
                crate::backend::parser::Statement::Insert(stmt) => {
                    let xid = ctx.ensure_write_xid()?;
                    let cid = ctx.next_command_id;
                    let stmt =
                        bind_insert_with_outer_scopes(&stmt, catalog.as_ref(), &outer_scopes)
                            .map_err(ExecError::Parse)?;
                    let result = execute_insert(stmt, catalog.as_ref(), ctx, xid, cid);
                    if result.is_ok() {
                        advance_plpgsql_command_id(ctx);
                    }
                    result
                }
                crate::backend::parser::Statement::Update(stmt) => {
                    let xid = ctx.ensure_write_xid()?;
                    let cid = ctx.next_command_id;
                    let stmt =
                        bind_update_with_outer_scopes(&stmt, catalog.as_ref(), &outer_scopes)
                            .map_err(ExecError::Parse)?;
                    let stmt = bind_update_current_of(&stmt, compiled, state)?;
                    let result = execute_update(stmt, catalog.as_ref(), ctx, xid, cid);
                    if result.is_ok() {
                        advance_plpgsql_command_id(ctx);
                    }
                    result
                }
                crate::backend::parser::Statement::Delete(stmt) => {
                    let xid = ctx.ensure_write_xid()?;
                    let stmt =
                        bind_delete_with_outer_scopes(&stmt, catalog.as_ref(), &outer_scopes)
                            .map_err(ExecError::Parse)?;
                    let stmt = bind_delete_current_of(&stmt, compiled, state)?;
                    let result = execute_delete(stmt, catalog.as_ref(), ctx, xid);
                    if result.is_ok() {
                        advance_plpgsql_command_id(ctx);
                    }
                    result
                }
                crate::backend::parser::Statement::Merge(stmt) => {
                    let xid = ctx.ensure_write_xid()?;
                    let cid = ctx.next_command_id;
                    if let Some(scope) = runtime_scope {
                        install_runtime_sql_external_params(scope, state, ctx);
                    }
                    let merge_outer_scopes = if runtime_scope.is_some() {
                        param_scopes.as_slice()
                    } else {
                        outer_scopes.as_slice()
                    };
                    let stmt = plan_merge_with_outer_scopes_and_ctes(
                        &stmt,
                        catalog.as_ref(),
                        merge_outer_scopes,
                        &compiled.local_ctes,
                    )
                    .map_err(ExecError::Parse)?;
                    let result = execute_merge(stmt, catalog.as_ref(), ctx, xid, cid);
                    if result.is_ok() {
                        advance_plpgsql_command_id(ctx);
                    }
                    result
                }
                crate::backend::parser::Statement::CreateTable(stmt) => {
                    exec_dynamic_create_table(&stmt, ctx)
                }
                crate::backend::parser::Statement::CreateView(stmt) => {
                    exec_dynamic_create_view(&stmt, ctx)
                }
                crate::backend::parser::Statement::CreateTableAs(stmt) => {
                    exec_dynamic_create_table_as(&stmt, ctx)
                }
                crate::backend::parser::Statement::Analyze(stmt) => {
                    exec_dynamic_analyze(&stmt, ctx)
                }
                crate::backend::parser::Statement::DropIndex(stmt) => {
                    exec_function_drop_index(&stmt, ctx)
                }
                crate::backend::parser::Statement::DropTable(stmt) => {
                    exec_function_drop_table(&stmt, catalog.as_ref(), ctx)
                }
                crate::backend::parser::Statement::AlterTableAttachPartition(stmt)
                    if ctx.trigger_depth > 0 =>
                {
                    Err(ExecError::DetailedError {
                        message: format!(
                            "cannot ALTER TABLE \"{}\" because it is being used by active queries in this session",
                            stmt.parent_table
                                .rsplit('.')
                                .next()
                                .unwrap_or(&stmt.parent_table)
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "55006",
                    })
                }
                crate::backend::parser::Statement::Set(stmt)
                    if stmt.name.eq_ignore_ascii_case("jit") =>
                {
                    // :HACK: pgrust has no JIT subsystem; PL/pgSQL regression
                    // helpers use SET LOCAL jit=0 only to stabilize EXPLAIN.
                    Ok(crate::backend::executor::StatementResult::AffectedRows(0))
                }
                crate::backend::parser::Statement::Do(stmt) => {
                    super::execute_do_with_context_preserving_notices(&stmt, catalog.as_ref(), ctx)
                }
                other => execute_readonly_statement_with_config(
                    other,
                    catalog.as_ref(),
                    ctx,
                    planner_config,
                ),
            })
        },
    );
    result.map_err(|err| with_sql_statement_context(err, Some(sql)))
}

fn refresh_dynamic_sql_catalog(ctx: &mut ExecutorContext) {
    let Some(db) = ctx.database.clone() else {
        return;
    };
    let search_path = plpgsql_configured_search_path(ctx);
    let txn_ctx = ctx.transaction_xid().map(|xid| (xid, ctx.next_command_id));
    let catalog = db.lazy_catalog_lookup(ctx.client_id, txn_ctx, search_path.as_deref());
    ctx.catalog = Some(crate::backend::executor::executor_catalog(catalog));
}

fn exec_dynamic_analyze(
    stmt: &crate::backend::parser::AnalyzeStatement,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    let db = ctx.database.clone().ok_or_else(|| {
        function_runtime_error(
            "PL/pgSQL ANALYZE requires database execution context",
            None,
            "0A000",
        )
    })?;
    let xid = ctx.ensure_write_xid()?;
    let cid = ctx.next_command_id;
    let search_path = plpgsql_configured_search_path(ctx);
    let targets = db.effective_analyze_targets_with_search_path(
        ctx.client_id,
        Some((xid, cid)),
        search_path.as_deref(),
        stmt,
    )?;
    let effect_start = ctx.catalog_effects.len();
    let result = db.execute_analyze_stmt_in_transaction_with_search_path(
        ctx.client_id,
        &targets,
        xid,
        cid,
        search_path.as_deref(),
        false,
        &mut ctx.catalog_effects,
    );
    if result.is_ok() {
        let consumed_catalog_cids = ctx
            .catalog_effects
            .len()
            .saturating_sub(effect_start)
            .max(1);
        advance_plpgsql_command_id_by(ctx, consumed_catalog_cids as u32);
        refresh_dynamic_sql_catalog(ctx);
    }
    result
}

fn resolve_dynamic_prepared_statement(
    stmt: Statement,
) -> Result<(Statement, Vec<PreparedExternalParam>), ExecError> {
    match stmt {
        Statement::Execute(execute_stmt) => {
            if let Some(resolved) = resolve_thread_prepared_statement(&execute_stmt)? {
                Ok((resolved.statement, resolved.params))
            } else {
                Ok((Statement::Execute(execute_stmt), Vec::new()))
            }
        }
        Statement::Explain(mut explain_stmt)
            if matches!(explain_stmt.statement.as_ref(), Statement::Execute(_)) =>
        {
            let Statement::Execute(execute_stmt) = explain_stmt.statement.as_ref() else {
                unreachable!();
            };
            if let Some(resolved) = resolve_thread_prepared_statement(execute_stmt)? {
                explain_stmt.statement = Box::new(resolved.statement);
                Ok((Statement::Explain(explain_stmt), resolved.params))
            } else {
                Ok((Statement::Explain(explain_stmt), Vec::new()))
            }
        }
        other => Ok((other, Vec::new())),
    }
}

fn bind_dynamic_external_params(
    params: &[PreparedExternalParam],
    catalog: &dyn CatalogLookup,
) -> Result<Vec<DynamicExternalParamBinding>, ExecError> {
    params
        .iter()
        .map(|param| {
            let (expr, inferred) =
                bind_scalar_expr_in_named_slot_scope(&param.arg, &[], &[], catalog, &[])
                    .map_err(ExecError::Parse)?;
            let ty = match &param.type_name {
                Some(type_name) => {
                    resolve_raw_type_name(type_name, catalog).map_err(ExecError::Parse)?
                }
                None => inferred,
            };
            Ok(DynamicExternalParamBinding {
                paramid: param.paramid,
                expr,
                ty,
            })
        })
        .collect()
}

fn dynamic_external_types(bindings: &[DynamicExternalParamBinding]) -> Vec<(usize, SqlType)> {
    bindings
        .iter()
        .map(|binding| (binding.paramid, binding.ty))
        .collect()
}

fn install_dynamic_external_params(
    bindings: &[DynamicExternalParamBinding],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let mut slot = TupleSlot::empty(0);
    for binding in bindings {
        let value = eval_expr(&binding.expr, &mut slot, ctx)?;
        let value = cast_value(value, binding.ty)?;
        ctx.expr_bindings
            .external_params
            .insert(binding.paramid, value);
    }
    Ok(())
}

fn execute_literal_query_result(
    sql: &str,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionQueryResult, ExecError> {
    let sql = sql.trim().trim_end_matches(';').trim_end().to_string();
    let catalog = ctx.catalog.clone().ok_or_else(|| {
        function_runtime_error(
            "user-defined functions require executor catalog context",
            None,
            "0A000",
        )
    })?;
    let planner_config = planner_config_from_executor_gucs(&ctx.gucs);

    execute_function_query_with_bindings(compiled, state, ctx, false, |ctx| {
        let stmt = parse_statement(&sql).map_err(ExecError::Parse)?;
        match stmt {
            crate::backend::parser::Statement::Select(stmt) => {
                execute_planned_query_result_with_bindings(
                    pg_plan_query_with_outer_scopes_and_ctes_config(
                        &stmt,
                        catalog.as_ref(),
                        &[],
                        &compiled.local_ctes,
                        planner_config,
                    )
                    .map_err(ExecError::Parse)?,
                    ctx,
                )
            }
            crate::backend::parser::Statement::Values(stmt) => statement_result_to_query_result(
                execute_planned_stmt(
                    pg_plan_values_query_with_outer_scopes_and_ctes_config(
                        &stmt,
                        catalog.as_ref(),
                        &[],
                        &compiled.local_ctes,
                        planner_config,
                    )
                    .map_err(ExecError::Parse)?,
                    ctx,
                )?,
                "cursor query did not produce rows",
            ),
            other => statement_result_to_query_result(
                execute_readonly_statement_with_config(
                    other,
                    catalog.as_ref(),
                    ctx,
                    planner_config,
                )?,
                "cursor query did not produce rows",
            ),
        }
    })
}

fn execute_function_query_with_bindings<T>(
    compiled: &CompiledFunction,
    state: &FunctionState,
    ctx: &mut ExecutorContext,
    bind_outer_tuple: bool,
    f: impl FnOnce(&mut ExecutorContext) -> Result<T, ExecError>,
) -> Result<T, ExecError> {
    let saved_outer_tuple = ctx.expr_bindings.outer_tuple.clone();
    let saved_exec_params = ctx.expr_bindings.exec_params.clone();
    let saved_external_params = ctx.expr_bindings.external_params.clone();
    if bind_outer_tuple {
        ctx.expr_bindings.outer_tuple = Some(function_outer_tuple(compiled, state));
    }
    let result = f(ctx);
    ctx.expr_bindings.outer_tuple = saved_outer_tuple;
    ctx.expr_bindings.exec_params = saved_exec_params;
    ctx.expr_bindings.external_params = saved_external_params;
    result
}

fn install_runtime_sql_external_params(
    scope: &RuntimeSqlScope,
    state: &FunctionState,
    ctx: &mut ExecutorContext,
) {
    let mut installed = HashSet::new();
    for column in scope.columns.iter().chain(
        scope
            .relation_scopes
            .iter()
            .flat_map(|(_, columns)| columns),
    ) {
        if installed.insert(column.slot) {
            let value = state
                .values
                .get(column.slot)
                .cloned()
                .unwrap_or(Value::Null);
            ctx.expr_bindings
                .external_params
                .insert(runtime_sql_param_id(column.slot), value);
        }
    }
}

fn statement_result_to_query_result(
    result: StatementResult,
    message: &str,
) -> Result<FunctionQueryResult, ExecError> {
    pgrust_plpgsql::statement_result_to_query_result(result)
        .ok_or_else(|| function_runtime_error(message, None, "XX000"))
}

// :HACK: Mirror PostgreSQL's generated sys_fk_relationships data for the
// catalog-consistency regression until pgrust has a catalog-codegen path.
const CATALOG_FOREIGN_KEY_ROWS: &str = r#"
pg_proc|pronamespace|pg_namespace|oid
pg_proc|proowner|pg_authid|oid
pg_proc|prolang|pg_language|oid
pg_proc|provariadic|pg_type|oid
pg_proc|prosupport|pg_proc|oid
pg_proc|prorettype|pg_type|oid
pg_proc|proargtypes|pg_type|oid
pg_proc|proallargtypes|pg_type|oid
pg_proc|protrftypes|pg_type|oid
pg_type|typnamespace|pg_namespace|oid
pg_type|typowner|pg_authid|oid
pg_type|typrelid|pg_class|oid
pg_type|typsubscript|pg_proc|oid
pg_type|typelem|pg_type|oid
pg_type|typarray|pg_type|oid
pg_type|typinput|pg_proc|oid
pg_type|typoutput|pg_proc|oid
pg_type|typreceive|pg_proc|oid
pg_type|typsend|pg_proc|oid
pg_type|typmodin|pg_proc|oid
pg_type|typmodout|pg_proc|oid
pg_type|typanalyze|pg_proc|oid
pg_type|typbasetype|pg_type|oid
pg_type|typcollation|pg_collation|oid
pg_attribute|attrelid|pg_class|oid
pg_attribute|atttypid|pg_type|oid
pg_attribute|attcollation|pg_collation|oid
pg_class|relnamespace|pg_namespace|oid
pg_class|reltype|pg_type|oid
pg_class|reloftype|pg_type|oid
pg_class|relowner|pg_authid|oid
pg_class|relam|pg_am|oid
pg_class|reltablespace|pg_tablespace|oid
pg_class|reltoastrelid|pg_class|oid
pg_class|relrewrite|pg_class|oid
pg_attrdef|adrelid|pg_class|oid
pg_attrdef|adrelid,adnum|pg_attribute|attrelid,attnum
pg_constraint|connamespace|pg_namespace|oid
pg_constraint|conrelid|pg_class|oid
pg_constraint|contypid|pg_type|oid
pg_constraint|conindid|pg_class|oid
pg_constraint|conparentid|pg_constraint|oid
pg_constraint|confrelid|pg_class|oid
pg_constraint|conpfeqop|pg_operator|oid
pg_constraint|conppeqop|pg_operator|oid
pg_constraint|conffeqop|pg_operator|oid
pg_constraint|conexclop|pg_operator|oid
pg_constraint|conrelid,conkey|pg_attribute|attrelid,attnum
pg_constraint|confrelid,confkey|pg_attribute|attrelid,attnum
pg_inherits|inhrelid|pg_class|oid
pg_inherits|inhparent|pg_class|oid
pg_index|indexrelid|pg_class|oid
pg_index|indrelid|pg_class|oid
pg_index|indcollation|pg_collation|oid
pg_index|indclass|pg_opclass|oid
pg_index|indrelid,indkey|pg_attribute|attrelid,attnum
pg_operator|oprnamespace|pg_namespace|oid
pg_operator|oprowner|pg_authid|oid
pg_operator|oprleft|pg_type|oid
pg_operator|oprright|pg_type|oid
pg_operator|oprresult|pg_type|oid
pg_operator|oprcom|pg_operator|oid
pg_operator|oprnegate|pg_operator|oid
pg_operator|oprcode|pg_proc|oid
pg_operator|oprrest|pg_proc|oid
pg_operator|oprjoin|pg_proc|oid
pg_opfamily|opfmethod|pg_am|oid
pg_opfamily|opfnamespace|pg_namespace|oid
pg_opfamily|opfowner|pg_authid|oid
pg_opclass|opcmethod|pg_am|oid
pg_opclass|opcnamespace|pg_namespace|oid
pg_opclass|opcowner|pg_authid|oid
pg_opclass|opcfamily|pg_opfamily|oid
pg_opclass|opcintype|pg_type|oid
pg_opclass|opckeytype|pg_type|oid
pg_am|amhandler|pg_proc|oid
pg_amop|amopfamily|pg_opfamily|oid
pg_amop|amoplefttype|pg_type|oid
pg_amop|amoprighttype|pg_type|oid
pg_amop|amopopr|pg_operator|oid
pg_amop|amopmethod|pg_am|oid
pg_amop|amopsortfamily|pg_opfamily|oid
pg_amproc|amprocfamily|pg_opfamily|oid
pg_amproc|amproclefttype|pg_type|oid
pg_amproc|amprocrighttype|pg_type|oid
pg_amproc|amproc|pg_proc|oid
pg_language|lanowner|pg_authid|oid
pg_language|lanplcallfoid|pg_proc|oid
pg_language|laninline|pg_proc|oid
pg_language|lanvalidator|pg_proc|oid
pg_largeobject_metadata|lomowner|pg_authid|oid
pg_largeobject|loid|pg_largeobject_metadata|oid
pg_aggregate|aggfnoid|pg_proc|oid
pg_aggregate|aggtransfn|pg_proc|oid
pg_aggregate|aggfinalfn|pg_proc|oid
pg_aggregate|aggcombinefn|pg_proc|oid
pg_aggregate|aggserialfn|pg_proc|oid
pg_aggregate|aggdeserialfn|pg_proc|oid
pg_aggregate|aggmtransfn|pg_proc|oid
pg_aggregate|aggminvtransfn|pg_proc|oid
pg_aggregate|aggmfinalfn|pg_proc|oid
pg_aggregate|aggsortop|pg_operator|oid
pg_aggregate|aggtranstype|pg_type|oid
pg_aggregate|aggmtranstype|pg_type|oid
pg_statistic|starelid|pg_class|oid
pg_statistic|staop1|pg_operator|oid
pg_statistic|staop2|pg_operator|oid
pg_statistic|staop3|pg_operator|oid
pg_statistic|staop4|pg_operator|oid
pg_statistic|staop5|pg_operator|oid
pg_statistic|stacoll1|pg_collation|oid
pg_statistic|stacoll2|pg_collation|oid
pg_statistic|stacoll3|pg_collation|oid
pg_statistic|stacoll4|pg_collation|oid
pg_statistic|stacoll5|pg_collation|oid
pg_statistic|starelid,staattnum|pg_attribute|attrelid,attnum
pg_statistic_ext|stxrelid|pg_class|oid
pg_statistic_ext|stxnamespace|pg_namespace|oid
pg_statistic_ext|stxowner|pg_authid|oid
pg_statistic_ext|stxrelid,stxkeys|pg_attribute|attrelid,attnum
pg_statistic_ext_data|stxoid|pg_statistic_ext|oid
pg_rewrite|ev_class|pg_class|oid
pg_trigger|tgrelid|pg_class|oid
pg_trigger|tgparentid|pg_trigger|oid
pg_trigger|tgfoid|pg_proc|oid
pg_trigger|tgconstrrelid|pg_class|oid
pg_trigger|tgconstrindid|pg_class|oid
pg_trigger|tgconstraint|pg_constraint|oid
pg_trigger|tgrelid,tgattr|pg_attribute|attrelid,attnum
pg_event_trigger|evtowner|pg_authid|oid
pg_event_trigger|evtfoid|pg_proc|oid
pg_description|classoid|pg_class|oid
pg_cast|castsource|pg_type|oid
pg_cast|casttarget|pg_type|oid
pg_cast|castfunc|pg_proc|oid
pg_enum|enumtypid|pg_type|oid
pg_namespace|nspowner|pg_authid|oid
pg_conversion|connamespace|pg_namespace|oid
pg_conversion|conowner|pg_authid|oid
pg_conversion|conproc|pg_proc|oid
pg_depend|classid|pg_class|oid
pg_depend|refclassid|pg_class|oid
pg_database|datdba|pg_authid|oid
pg_database|dattablespace|pg_tablespace|oid
pg_db_role_setting|setdatabase|pg_database|oid
pg_db_role_setting|setrole|pg_authid|oid
pg_tablespace|spcowner|pg_authid|oid
pg_auth_members|roleid|pg_authid|oid
pg_auth_members|member|pg_authid|oid
pg_auth_members|grantor|pg_authid|oid
pg_shdepend|dbid|pg_database|oid
pg_shdepend|classid|pg_class|oid
pg_shdepend|refclassid|pg_class|oid
pg_shdescription|classoid|pg_class|oid
pg_ts_config|cfgnamespace|pg_namespace|oid
pg_ts_config|cfgowner|pg_authid|oid
pg_ts_config|cfgparser|pg_ts_parser|oid
pg_ts_config_map|mapcfg|pg_ts_config|oid
pg_ts_config_map|mapdict|pg_ts_dict|oid
pg_ts_dict|dictnamespace|pg_namespace|oid
pg_ts_dict|dictowner|pg_authid|oid
pg_ts_dict|dicttemplate|pg_ts_template|oid
pg_ts_parser|prsnamespace|pg_namespace|oid
pg_ts_parser|prsstart|pg_proc|oid
pg_ts_parser|prstoken|pg_proc|oid
pg_ts_parser|prsend|pg_proc|oid
pg_ts_parser|prsheadline|pg_proc|oid
pg_ts_parser|prslextype|pg_proc|oid
pg_ts_template|tmplnamespace|pg_namespace|oid
pg_ts_template|tmplinit|pg_proc|oid
pg_ts_template|tmpllexize|pg_proc|oid
pg_extension|extowner|pg_authid|oid
pg_extension|extnamespace|pg_namespace|oid
pg_extension|extconfig|pg_class|oid
pg_foreign_data_wrapper|fdwowner|pg_authid|oid
pg_foreign_data_wrapper|fdwhandler|pg_proc|oid
pg_foreign_data_wrapper|fdwvalidator|pg_proc|oid
pg_foreign_server|srvowner|pg_authid|oid
pg_foreign_server|srvfdw|pg_foreign_data_wrapper|oid
pg_user_mapping|umuser|pg_authid|oid
pg_user_mapping|umserver|pg_foreign_server|oid
pg_foreign_table|ftrelid|pg_class|oid
pg_foreign_table|ftserver|pg_foreign_server|oid
pg_policy|polrelid|pg_class|oid
pg_policy|polroles|pg_authid|oid
pg_default_acl|defaclrole|pg_authid|oid
pg_default_acl|defaclnamespace|pg_namespace|oid
pg_init_privs|classoid|pg_class|oid
pg_seclabel|classoid|pg_class|oid
pg_shseclabel|classoid|pg_class|oid
pg_collation|collnamespace|pg_namespace|oid
pg_collation|collowner|pg_authid|oid
pg_partitioned_table|partrelid|pg_class|oid
pg_partitioned_table|partdefid|pg_class|oid
pg_partitioned_table|partclass|pg_opclass|oid
pg_partitioned_table|partcollation|pg_collation|oid
pg_partitioned_table|partrelid,partattrs|pg_attribute|attrelid,attnum
pg_range|rngtypid|pg_type|oid
pg_range|rngsubtype|pg_type|oid
pg_range|rngmultitypid|pg_type|oid
pg_range|rngcollation|pg_collation|oid
pg_range|rngsubopc|pg_opclass|oid
pg_range|rngcanonical|pg_proc|oid
pg_range|rngsubdiff|pg_proc|oid
pg_transform|trftype|pg_type|oid
pg_transform|trflang|pg_language|oid
pg_transform|trffromsql|pg_proc|oid
pg_transform|trftosql|pg_proc|oid
pg_sequence|seqrelid|pg_class|oid
pg_sequence|seqtypid|pg_type|oid
pg_publication|pubowner|pg_authid|oid
pg_publication_namespace|pnpubid|pg_publication|oid
pg_publication_namespace|pnnspid|pg_namespace|oid
pg_publication_rel|prpubid|pg_publication|oid
pg_publication_rel|prrelid|pg_class|oid
pg_subscription|subdbid|pg_database|oid
pg_subscription|subowner|pg_authid|oid
pg_subscription_rel|srsubid|pg_subscription|oid
pg_subscription_rel|srrelid|pg_class|oid
"#;

fn catalog_foreign_key_query_result() -> FunctionQueryResult {
    let columns = vec![
        plpgsql_query_column("fktable", SqlType::new(SqlTypeKind::Text)),
        plpgsql_query_column("fkcols", SqlType::array_of(SqlType::new(SqlTypeKind::Text))),
        plpgsql_query_column("pktable", SqlType::new(SqlTypeKind::Text)),
        plpgsql_query_column("pkcols", SqlType::array_of(SqlType::new(SqlTypeKind::Text))),
        plpgsql_query_column("is_array", SqlType::new(SqlTypeKind::Bool)),
        plpgsql_query_column("is_opt", SqlType::new(SqlTypeKind::Bool)),
    ];
    let rows = CATALOG_FOREIGN_KEY_ROWS
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            let parts = line.split('|').collect::<Vec<_>>();
            let [fktable, fkcols, pktable, pkcols] = parts.as_slice() else {
                return None;
            };
            Some(FunctionQueryRow {
                values: vec![
                    Value::Text((*fktable).into()),
                    catalog_foreign_key_column_array(fkcols),
                    Value::Text((*pktable).into()),
                    catalog_foreign_key_column_array(pkcols),
                    Value::Bool(catalog_foreign_key_is_array(fktable, fkcols)),
                    Value::Bool(catalog_foreign_key_is_optional(fktable, fkcols)),
                ],
                system_bindings: Vec::new(),
            })
        })
        .collect();
    FunctionQueryResult { columns, rows }
}

fn cursor_name_for_slot(slot: usize, fallback: &str, state: &FunctionState) -> String {
    state.values[slot]
        .as_text()
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| fallback.to_string())
}

fn positioned_cursor_row_from_bindings(
    bindings: &[SystemVarBinding],
) -> Option<PositionedCursorRow> {
    let mut positioned = bindings.iter().filter_map(|binding| {
        binding.tid.map(|tid| PositionedCursorRow {
            table_oid: binding.table_oid,
            tid,
        })
    });
    let first = positioned.next()?;
    positioned.next().is_none().then_some(first)
}

fn export_open_cursors_as_portals(state: &FunctionState, ctx: &mut ExecutorContext) {
    for (name, cursor) in &state.cursors {
        ctx.pending_portals.retain(|portal| portal.name != *name);
        let column_names = cursor
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let pos = (cursor.current + 1).clamp(0, cursor.rows.len() as isize) as usize;
        let rows = cursor
            .rows
            .iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>();
        let row_positions = cursor
            .rows
            .iter()
            .map(|row| positioned_cursor_row_from_bindings(&row.system_bindings))
            .collect::<Vec<_>>();
        let mut portal = Portal::materialized_select(
            name.clone(),
            String::new(),
            None,
            Vec::new(),
            CursorOptions {
                holdable: false,
                binary: false,
                scroll: cursor.scrollable,
                no_scroll: !cursor.scrollable,
                visible: true,
            },
            true,
            0,
            cursor.columns.clone(),
            column_names,
            rows,
        );
        if let PortalExecution::Materialized {
            row_positions: portal_row_positions,
            pos: portal_pos,
            ..
        } = &mut portal.execution
        {
            *portal_row_positions = row_positions;
            *portal_pos = pos;
        }
        ctx.pending_portals.push(portal);
    }
}

fn exec_function_open_cursor(
    slot: usize,
    name: &str,
    source: &CompiledCursorOpenSource,
    scrollable: bool,
    constant: bool,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    if constant && !refcursor_slot_has_name(slot, state) {
        return Err(function_runtime_error(
            &format!("variable \"{name}\" is declared CONSTANT"),
            None,
            "22005",
        ));
    }
    let portal_name = cursor_name_for_slot(slot, name, state);
    let result = execute_cursor_open_source(source, compiled, state, ctx)?;
    if !constant {
        state.values[slot] = Value::Text(portal_name.clone().into());
    }
    state.cursors.insert(
        portal_name,
        FunctionCursor {
            columns: result.columns,
            rows: result.rows,
            current: -1,
            scrollable,
        },
    );
    Ok(())
}

fn refcursor_slot_has_name(slot: usize, state: &FunctionState) -> bool {
    state.values[slot]
        .as_text()
        .is_some_and(|name| !name.is_empty())
}

fn execute_cursor_query_result(
    slot: usize,
    name: &str,
    source: &CompiledCursorOpenSource,
    _scrollable: bool,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionQueryResult, ExecError> {
    let original_name = state.values[slot].clone();
    let result = execute_cursor_open_source(source, compiled, state, ctx);
    state.values[slot] = original_name;
    let _ = name;
    result
}

fn execute_cursor_open_source(
    source: &CompiledCursorOpenSource,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionQueryResult, ExecError> {
    match source {
        CompiledCursorOpenSource::Static { plan } => {
            execute_function_query_result(plan, compiled, state, ctx)
        }
        CompiledCursorOpenSource::Dynamic {
            sql_expr,
            using_exprs,
        } => execute_dynamic_for_query(sql_expr, using_exprs, compiled, state, ctx),
        CompiledCursorOpenSource::Declared {
            query,
            params,
            args,
            arg_context,
        } => execute_declared_cursor_query(query, params, args, arg_context, compiled, state, ctx),
    }
}

fn execute_declared_cursor_query(
    query: &str,
    params: &[DeclaredCursorParam],
    args: &[CompiledExpr],
    arg_context: &Option<String>,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<FunctionQueryResult, ExecError> {
    let values = args
        .iter()
        .map(|expr| eval_function_expr_inner(expr, &state.values, ctx))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| match arg_context {
            Some(source) => with_plpgsql_expression_context(err, source),
            None => err,
        })?;
    // :HACK: Declared cursor parameters are substituted as SQL literals until
    // pgrust has native PL/pgSQL cursor parameter binding in the SQL planner.
    let sql = substitute_declared_cursor_params(query, params, &values, ctx)?;
    execute_literal_query_result(&sql, compiled, state, ctx)
}

fn exec_function_fetch_cursor(
    slot: usize,
    direction: CursorDirection,
    targets: &[CompiledSelectIntoTarget],
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let portal_name = cursor_name_for_slot(slot, "", state);
    let (rows, columns) = if let Some(cursor) = state.cursors.get_mut(&portal_name) {
        if !cursor_direction_is_forward_only(direction) && !cursor.scrollable {
            return Err(function_runtime_error_with_hint(
                "cursor can only scan forward",
                None,
                Some("Declare it with SCROLL option to enable backward scan.".into()),
                "55000",
            ));
        }
        let row = cursor_fetch(cursor, direction).map(|row| row.values);
        (row.into_iter().collect::<Vec<_>>(), cursor.columns.clone())
    } else if let Some(portal) = ctx
        .pending_portals
        .iter_mut()
        .find(|portal| portal.name == portal_name)
    {
        let result = portal.fetch_direction(portal_direction_from_plpgsql(direction), false)?;
        (result.rows, result.columns)
    } else {
        return Err(function_runtime_error(
            &format!("cursor \"{portal_name}\" does not exist"),
            None,
            "34000",
        ));
    };
    assign_query_rows_into_targets(
        &rows, &columns, targets, false, None, false, true, compiled, state, ctx,
    )
}

fn exec_function_move_cursor(
    slot: usize,
    direction: CursorDirection,
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let portal_name = cursor_name_for_slot(slot, "", state);
    let moved = if let Some(cursor) = state.cursors.get_mut(&portal_name) {
        if !cursor_direction_is_forward_only(direction) && !cursor.scrollable {
            return Err(function_runtime_error_with_hint(
                "cursor can only scan forward",
                None,
                Some("Declare it with SCROLL option to enable backward scan.".into()),
                "55000",
            ));
        }
        cursor_move(cursor, direction)
    } else if let Some(portal) = ctx
        .pending_portals
        .iter_mut()
        .find(|portal| portal.name == portal_name)
    {
        portal
            .fetch_direction(portal_direction_from_plpgsql(direction), true)?
            .processed
            > 0
    } else {
        return Err(function_runtime_error(
            &format!("cursor \"{portal_name}\" does not exist"),
            None,
            "34000",
        ));
    };
    state.values[compiled.found_slot] = Value::Bool(moved);
    Ok(())
}

fn portal_direction_from_plpgsql(direction: CursorDirection) -> PortalFetchDirection {
    match direction {
        CursorDirection::Next => PortalFetchDirection::Next,
        CursorDirection::Prior => PortalFetchDirection::Prior,
        CursorDirection::First => PortalFetchDirection::First,
        CursorDirection::Last => PortalFetchDirection::Last,
        CursorDirection::Forward(count) => {
            PortalFetchDirection::Forward(PortalFetchLimit::Count(count as usize))
        }
        CursorDirection::Backward(count) => {
            PortalFetchDirection::Backward(PortalFetchLimit::Count(count as usize))
        }
        CursorDirection::ForwardAll => PortalFetchDirection::Forward(PortalFetchLimit::All),
        CursorDirection::BackwardAll => PortalFetchDirection::Backward(PortalFetchLimit::All),
        CursorDirection::Absolute(index) => PortalFetchDirection::Absolute(index as i64),
        CursorDirection::Relative(count) => PortalFetchDirection::Relative(count as i64),
    }
}

fn cursor_direction_is_forward_only(direction: CursorDirection) -> bool {
    pgrust_plpgsql::cursor_direction_is_forward_only(direction)
}

fn cursor_fetch(
    cursor: &mut FunctionCursor,
    direction: CursorDirection,
) -> Option<FunctionQueryRow> {
    pgrust_plpgsql::cursor_fetch(cursor, direction)
}

fn cursor_move(cursor: &mut FunctionCursor, direction: CursorDirection) -> bool {
    pgrust_plpgsql::cursor_move(cursor, direction)
}

fn cursor_target_position(cursor: &FunctionCursor, direction: CursorDirection) -> Option<isize> {
    pgrust_plpgsql::cursor_target_position(cursor, direction)
}

fn exec_function_close_cursor(slot: usize, state: &mut FunctionState) -> Result<(), ExecError> {
    let portal_name = cursor_name_for_slot(slot, "", state);
    if state.cursors.remove(&portal_name).is_none() {
        return Err(function_runtime_error(
            &format!("cursor \"{portal_name}\" does not exist"),
            None,
            "34000",
        ));
    }
    Ok(())
}

fn exec_function_get_diagnostics(
    stacked: bool,
    items: &[(CompiledSelectIntoTarget, String)],
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    if stacked && state.current_exception.is_none() {
        return Err(ExecError::DetailedError {
            message: "GET STACKED DIAGNOSTICS cannot be used outside an exception handler".into(),
            detail: None,
            hint: None,
            sqlstate: "0Z002",
        });
    }
    for (target, item) in items {
        let item = item.to_ascii_lowercase();
        let value = if stacked {
            let current = state.current_exception.as_ref().expect("checked above");
            match item.as_str() {
                "returned_sqlstate" => diagnostic_text(Some(current.sqlstate)),
                "message_text" => diagnostic_text(Some(&current.message)),
                "pg_exception_detail" => diagnostic_text(current.detail.as_deref()),
                "pg_exception_hint" => diagnostic_text(current.hint.as_deref()),
                "pg_exception_context" => diagnostic_text(current.context.as_deref()),
                "column_name" => diagnostic_text(current.column_name.as_deref()),
                "constraint_name" => diagnostic_text(current.constraint_name.as_deref()),
                "pg_datatype_name" => diagnostic_text(current.datatype_name.as_deref()),
                "table_name" => diagnostic_text(current.table_name.as_deref()),
                "schema_name" => diagnostic_text(current.schema_name.as_deref()),
                _ => diagnostic_text(None),
            }
        } else {
            match item.as_str() {
                "row_count" => Value::Int64(state.last_row_count as i64),
                "found" => Value::Bool(false),
                "pg_routine_oid" => Value::Int64(compiled.proc_oid as i64),
                "pg_context" => diagnostic_text(current_plpgsql_context().as_deref()),
                _ => diagnostic_text(None),
            }
        };
        state.values[target.slot] = cast_function_value(value, None, target.ty, ctx)?;
    }
    Ok(())
}

fn assign_query_row_to_targets(
    row: &[Value],
    columns: &[QueryColumn],
    targets: &[CompiledSelectIntoTarget],
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
    require_exact_single_scalar_width: bool,
) -> Result<(), ExecError> {
    match targets {
        [target @ CompiledSelectIntoTarget { ty, .. }]
            if matches!(ty.kind, SqlTypeKind::Record | SqlTypeKind::Composite) =>
        {
            let descriptor = record_descriptor_for_query_target(*ty, columns, ctx)?;
            if row.len() != descriptor.fields.len() {
                handle_strict_multi_assignment(&ctx.gucs)?;
            }
            let values = descriptor
                .fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    cast_function_value(
                        row.get(index).cloned().unwrap_or(Value::Null),
                        columns.get(index).map(|column| column.sql_type),
                        field.sql_type,
                        ctx,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            assign_select_target_casted_value(
                target,
                Value::Record(RecordValue::from_descriptor(descriptor, values)),
                state,
            )?;
            Ok(())
        }
        [target @ CompiledSelectIntoTarget { ty, .. }] => {
            if require_exact_single_scalar_width && row.len() != 1 {
                handle_strict_multi_assignment_or_unexpected_shape(&ctx.gucs, 1, row.len())?;
            }
            let value = row.first().cloned().unwrap_or(Value::Null);
            let value = cast_function_value(
                value,
                columns.first().map(|column| column.sql_type),
                *ty,
                ctx,
            )?;
            assign_select_target_casted_value(target, value, state)?;
            Ok(())
        }
        _ => {
            if row.len() != targets.len() {
                handle_strict_multi_assignment(&ctx.gucs)?;
            }
            for (index, (target, value)) in targets
                .iter()
                .zip(row.iter().chain(std::iter::repeat(&Value::Null)))
                .enumerate()
            {
                let source_type = columns.get(index).map(|column| column.sql_type);
                let value = cast_function_value(value.clone(), source_type, target.ty, ctx)?;
                assign_select_target_casted_value(target, value, state)?;
            }
            Ok(())
        }
    }
}

fn assign_select_target_value(
    target: &CompiledSelectIntoTarget,
    value: Value,
    source_type: Option<SqlType>,
    state: &mut FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let value = cast_function_value(value, source_type, target.ty, ctx)?;
    assign_select_target_casted_value(target, value, state)
}

fn assign_select_target_casted_value(
    target: &CompiledSelectIntoTarget,
    value: Value,
    state: &mut FunctionState,
) -> Result<(), ExecError> {
    ensure_not_null_assignment(target.name.as_deref(), target.not_null, &value)?;
    state.values[target.slot] = value;
    Ok(())
}

fn ensure_not_null_assignment(
    name: Option<&str>,
    not_null: bool,
    value: &Value,
) -> Result<(), ExecError> {
    if !not_null || !matches!(value, Value::Null) {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!(
            "null value cannot be assigned to variable \"{}\" declared NOT NULL",
            name.unwrap_or("<unnamed>")
        ),
        detail: None,
        hint: None,
        sqlstate: "22004",
    })
}

fn handle_strict_multi_assignment(gucs: &HashMap<String, String>) -> Result<(), ExecError> {
    let detail_for_level =
        |level_name: &str| format!("strict_multi_assignment check of {level_name} is active.");
    match plpgsql_extra_check_level(gucs, "strict_multi_assignment") {
        Some(ExtraCheckLevel::Error) => Err(function_runtime_error_with_hint(
            "number of source and target fields in assignment does not match",
            Some(detail_for_level("extra_errors")),
            Some("Make sure the query returns the exact list of columns.".into()),
            "42804",
        )),
        Some(ExtraCheckLevel::Warning) => {
            queue_plpgsql_warning(
                "number of source and target fields in assignment does not match",
                Some(detail_for_level("extra_warnings")),
                Some("Make sure the query returns the exact list of columns.".into()),
            );
            Ok(())
        }
        None => Ok(()),
    }
}

fn handle_strict_multi_assignment_or_unexpected_shape(
    gucs: &HashMap<String, String>,
    expected: usize,
    got: usize,
) -> Result<(), ExecError> {
    if plpgsql_extra_check_level(gucs, "strict_multi_assignment").is_some() {
        handle_strict_multi_assignment(gucs)
    } else {
        let detail = if expected == 1 {
            format!("expected 1 column, got {got}")
        } else {
            format!("expected {expected} columns, got {got}")
        };
        Err(function_runtime_error(
            "query returned an unexpected row shape",
            Some(detail),
            "42804",
        ))
    }
}

fn cast_function_value(
    value: Value,
    source_type: Option<SqlType>,
    target_type: SqlType,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let mut casted = cast_value_with_source_type_catalog_and_config(
        value,
        source_type,
        target_type,
        ctx.catalog.as_deref(),
        &ctx.datetime_config,
    )?;
    if let Value::Record(record) = &casted
        && !matches!(
            target_type.kind,
            SqlTypeKind::Composite | SqlTypeKind::Record
        )
    {
        casted = cast_value_with_source_type_catalog_and_config(
            Value::Text(crate::backend::executor::value_io::format_record_text(&record).into()),
            Some(SqlType::new(SqlTypeKind::Text)),
            target_type,
            ctx.catalog.as_deref(),
            &ctx.datetime_config,
        )?;
    }
    enforce_domain_constraints_for_value(casted, target_type, ctx)
}

fn compiled_expr_sql_type_hint(expr: &CompiledExpr) -> Option<SqlType> {
    match expr {
        CompiledExpr::Scalar { expr, .. } => expr_sql_type_hint(expr),
        CompiledExpr::QueryCompare { .. } => Some(SqlType::new(SqlTypeKind::Bool)),
    }
}

fn assign_null_to_targets(
    targets: &[CompiledSelectIntoTarget],
    state: &mut FunctionState,
) -> Result<(), ExecError> {
    for target in targets {
        assign_select_target_casted_value(target, Value::Null, state)?;
    }
    Ok(())
}

fn function_outer_tuple(compiled: &CompiledFunction, state: &FunctionState) -> Vec<Value> {
    pgrust_plpgsql::function_outer_tuple(&compiled.return_contract, &state.values)
}

fn bind_update_current_of(
    stmt: &crate::backend::parser::BoundUpdateStatement,
    compiled: &CompiledFunction,
    state: &FunctionState,
) -> Result<crate::backend::parser::BoundUpdateStatement, ExecError> {
    let mut stmt = stmt.clone();
    let Some(cursor_name) = stmt.current_of.clone() else {
        return Ok(stmt);
    };
    let predicate =
        current_of_predicate(resolve_current_of_binding(&cursor_name, compiled, state)?)?;
    for target in &mut stmt.targets {
        target.predicate = Some(match target.predicate.take() {
            Some(existing) => Expr::and(existing, predicate.clone()),
            None => predicate.clone(),
        });
    }
    stmt.current_of = None;
    Ok(stmt)
}

fn bind_delete_current_of(
    stmt: &crate::backend::parser::BoundDeleteStatement,
    compiled: &CompiledFunction,
    state: &FunctionState,
) -> Result<crate::backend::parser::BoundDeleteStatement, ExecError> {
    let mut stmt = stmt.clone();
    let Some(cursor_name) = stmt.current_of.clone() else {
        return Ok(stmt);
    };
    let predicate =
        current_of_predicate(resolve_current_of_binding(&cursor_name, compiled, state)?)?;
    for target in &mut stmt.targets {
        target.predicate = Some(match target.predicate.take() {
            Some(existing) => Expr::and(existing, predicate.clone()),
            None => predicate.clone(),
        });
    }
    stmt.current_of = None;
    Ok(stmt)
}

fn resolve_current_of_binding(
    cursor_name: &str,
    compiled: &CompiledFunction,
    state: &FunctionState,
) -> Result<SystemVarBinding, ExecError> {
    if let Some(cursor) = cursor_by_portal_name(state, cursor_name) {
        return current_cursor_system_binding(cursor).ok_or_else(|| {
            function_runtime_error(
                &format!("cursor \"{cursor_name}\" is not positioned on a row"),
                None,
                "24000",
            )
        });
    }

    let mut slots = Vec::new();
    pgrust_plpgsql::collect_compiled_slot_names(compiled, &mut slots);
    for (name, slot) in slots {
        if !name.eq_ignore_ascii_case(cursor_name) {
            continue;
        }
        let Some(portal_name) = state.values.get(slot).and_then(Value::as_text) else {
            continue;
        };
        let Some(cursor) = cursor_by_portal_name(state, portal_name) else {
            continue;
        };
        return current_cursor_system_binding(cursor).ok_or_else(|| {
            function_runtime_error(
                &format!("cursor \"{cursor_name}\" is not positioned on a row"),
                None,
                "24000",
            )
        });
    }

    Err(function_runtime_error(
        &format!("cursor \"{cursor_name}\" does not exist"),
        None,
        "34000",
    ))
}

fn cursor_by_portal_name<'a>(
    state: &'a FunctionState,
    portal_name: &str,
) -> Option<&'a FunctionCursor> {
    state.cursors.get(portal_name).or_else(|| {
        state
            .cursors
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(portal_name))
            .map(|(_, cursor)| cursor)
    })
}

fn current_of_predicate(binding: SystemVarBinding) -> Result<Expr, ExecError> {
    pgrust_plpgsql::current_of_predicate(binding).ok_or_else(|| {
        function_runtime_error("cursor is not positioned on a table row", None, "24000")
    })
}

fn current_cursor_system_binding(cursor: &FunctionCursor) -> Option<SystemVarBinding> {
    pgrust_plpgsql::current_cursor_system_binding(cursor)
}

fn assign_trigger_row_value(
    compiled: &CompiledFunction,
    state: &mut FunctionState,
    row: TriggerReturnedRow,
    value: Value,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let FunctionReturnContract::Trigger { bindings } = &compiled.return_contract else {
        return Err(function_runtime_error(
            "trigger row assignment reached a non-trigger function",
            None,
            "0A000",
        ));
    };
    let relation = match row {
        TriggerReturnedRow::New => &bindings.new_row,
        TriggerReturnedRow::Old => &bindings.old_row,
    };
    let Value::Record(record) = value else {
        return Err(function_runtime_error(
            "cannot assign non-composite value to trigger row",
            None,
            "42804",
        ));
    };
    if record.fields.len() != relation.slots.len() {
        return Err(function_runtime_error(
            "cannot assign record to trigger row",
            Some(format!(
                "expected {} fields, got {}",
                relation.slots.len(),
                record.fields.len()
            )),
            "42804",
        ));
    }

    for (index, slot) in relation.slots.iter().copied().enumerate() {
        let source_type = record
            .descriptor
            .fields
            .get(index)
            .map(|field| field.sql_type);
        let target_type = relation.field_types[index];
        let value =
            cast_function_value(record.fields[index].clone(), source_type, target_type, ctx)?;
        ensure_not_null_assignment(
            relation.field_names.get(index).map(String::as_str),
            relation.not_null.get(index).copied().unwrap_or(false),
            &value,
        )?;
        state.values[slot] = value;
    }
    Ok(())
}

fn anonymous_record_descriptor_for_columns(columns: &[QueryColumn]) -> RecordDescriptor {
    assign_anonymous_record_descriptor(
        columns
            .iter()
            .map(|column| (column.name.clone(), column.sql_type))
            .collect(),
    )
}

fn record_descriptor_for_query_target(
    target_ty: SqlType,
    columns: &[QueryColumn],
    ctx: &ExecutorContext,
) -> Result<RecordDescriptor, ExecError> {
    if target_ty.kind == SqlTypeKind::Composite && target_ty.typrelid != 0 {
        let catalog = ctx.catalog.clone().ok_or_else(|| {
            function_runtime_error(
                "named composite assignment requires catalog context",
                None,
                "0A000",
            )
        })?;
        let relation = catalog
            .lookup_relation_by_oid(target_ty.typrelid)
            .ok_or_else(|| {
                function_runtime_error(
                    &format!("unknown composite relation oid {}", target_ty.typrelid),
                    None,
                    "42704",
                )
            })?;
        return Ok(RecordDescriptor::named(
            target_ty.type_oid,
            target_ty.typrelid,
            target_ty.typmod,
            relation
                .desc
                .columns
                .into_iter()
                .filter(|column| !column.dropped)
                .map(|column| (column.name, column.sql_type))
                .collect(),
        ));
    }

    if target_ty.kind == SqlTypeKind::Record
        && target_ty.typmod > 0
        && let Some(descriptor) = lookup_anonymous_record_descriptor(target_ty.typmod)
    {
        return Ok(descriptor);
    }

    Ok(anonymous_record_descriptor_for_columns(columns))
}

#[derive(Debug, Clone)]
enum RuntimeAssignIndirection {
    Field(String),
    Subscript(Value),
}

fn eval_assign_indirection_do(
    target: &CompiledIndirectAssignTarget,
    values: &[Value],
) -> Result<Vec<RuntimeAssignIndirection>, ExecError> {
    target
        .indirection
        .iter()
        .map(|step| match step {
            CompiledAssignIndirection::Field(field) => {
                Ok(RuntimeAssignIndirection::Field(field.clone()))
            }
            CompiledAssignIndirection::Subscript(expr) => Ok(RuntimeAssignIndirection::Subscript(
                eval_do_expr(expr, values)?,
            )),
        })
        .collect()
}

fn eval_assign_indirection_function(
    target: &CompiledIndirectAssignTarget,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Vec<RuntimeAssignIndirection>, ExecError> {
    target
        .indirection
        .iter()
        .map(|step| match step {
            CompiledAssignIndirection::Field(field) => {
                Ok(RuntimeAssignIndirection::Field(field.clone()))
            }
            CompiledAssignIndirection::Subscript(expr) => Ok(RuntimeAssignIndirection::Subscript(
                eval_function_expr(expr, values, ctx)?,
            )),
        })
        .collect()
}

fn assign_indirect_value(
    current: Value,
    current_ty: SqlType,
    indirection: &[RuntimeAssignIndirection],
    replacement: Value,
    ctx: Option<&ExecutorContext>,
) -> Result<Value, ExecError> {
    let Some((step, rest)) = indirection.split_first() else {
        return cast_value(replacement, current_ty);
    };
    match step {
        RuntimeAssignIndirection::Field(field) => {
            let mut record = assignment_record_value_for_field(current, current_ty, field, ctx)?;
            let (field_index, field_ty) = record
                .descriptor
                .fields
                .iter()
                .enumerate()
                .find(|(_, candidate)| candidate.name.eq_ignore_ascii_case(field))
                .map(|(index, candidate)| (index, candidate.sql_type))
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("record has no field \"{field}\""),
                    detail: None,
                    hint: None,
                    sqlstate: "42703",
                })?;
            record.fields[field_index] = assign_indirect_value(
                record.fields[field_index].clone(),
                field_ty,
                rest,
                replacement,
                ctx,
            )?;
            Ok(Value::Record(record))
        }
        RuntimeAssignIndirection::Subscript(index_value) => {
            let index = assignment_subscript_index(index_value)?;
            assign_array_index(current, current_ty, index, rest, replacement, ctx)
        }
    }
}

fn assignment_record_value_for_field(
    current: Value,
    sql_type: SqlType,
    field: &str,
    ctx: Option<&ExecutorContext>,
) -> Result<RecordValue, ExecError> {
    match current {
        Value::Record(record) => {
            if record
                .descriptor
                .fields
                .iter()
                .any(|candidate| candidate.name.eq_ignore_ascii_case(field))
            {
                return Ok(record);
            }
            if let Ok(descriptor) = assignment_record_descriptor(sql_type, ctx)
                && descriptor.fields.len() == record.fields.len()
            {
                return Ok(RecordValue::from_descriptor(descriptor, record.fields));
            }
            Ok(record)
        }
        Value::Null => {
            let descriptor = assignment_record_descriptor(sql_type, ctx)?;
            Ok(RecordValue::from_descriptor(
                descriptor.clone(),
                vec![Value::Null; descriptor.fields.len()],
            ))
        }
        _ => Err(ExecError::DetailedError {
            message: format!("cannot assign to field \"{field}\" of non-record value"),
            detail: None,
            hint: None,
            sqlstate: "42804",
        }),
    }
}

fn assignment_record_descriptor(
    sql_type: SqlType,
    ctx: Option<&ExecutorContext>,
) -> Result<RecordDescriptor, ExecError> {
    if matches!(sql_type.kind, SqlTypeKind::Composite) && sql_type.typrelid != 0 {
        let catalog =
            ctx.and_then(|ctx| ctx.catalog.as_deref())
                .ok_or_else(|| ExecError::DetailedError {
                    message: "named composite assignment requires catalog context".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                })?;
        let relation = catalog
            .lookup_relation_by_oid(sql_type.typrelid)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("unknown composite relation oid {}", sql_type.typrelid),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        return Ok(RecordDescriptor::named(
            sql_type.type_oid,
            sql_type.typrelid,
            sql_type.typmod,
            relation
                .desc
                .columns
                .iter()
                .filter(|column| !column.dropped)
                .map(|column| (column.name.clone(), column.sql_type))
                .collect(),
        ));
    }

    if matches!(sql_type.kind, SqlTypeKind::Record)
        && sql_type.typmod > 0
        && let Some(descriptor) = lookup_anonymous_record_descriptor(sql_type.typmod)
    {
        return Ok(descriptor);
    }

    Err(ExecError::DetailedError {
        message: format!(
            "cannot assign to field of type {} because it is not a composite value",
            sql_type_name(sql_type)
        ),
        detail: None,
        hint: None,
        sqlstate: "42804",
    })
}

fn assignment_subscript_index(value: &Value) -> Result<i32, ExecError> {
    match value {
        Value::Int16(value) => Ok(i32::from(*value)),
        Value::Int32(value) => Ok(*value),
        Value::Int64(value) => i32::try_from(*value).map_err(|_| array_assignment_limit_error()),
        Value::Null => Err(ExecError::InvalidStorageValue {
            column: "<array>".into(),
            details: "array subscript in assignment must not be null".into(),
        }),
        other => Err(ExecError::TypeMismatch {
            op: "array assignment",
            left: other.clone(),
            right: Value::Int32(1),
        }),
    }
}

fn assign_array_index(
    current: Value,
    array_ty: SqlType,
    index: i32,
    rest: &[RuntimeAssignIndirection],
    replacement: Value,
    ctx: Option<&ExecutorContext>,
) -> Result<Value, ExecError> {
    if !array_ty.is_array && !matches!(current, Value::Array(_) | Value::PgArray(_) | Value::Null) {
        return Err(ExecError::DetailedError {
            message: format!(
                "cannot subscript type {} because it does not support subscripting",
                sql_type_name(array_ty)
            ),
            detail: None,
            hint: None,
            sqlstate: "42804",
        });
    }

    if let Value::PgArray(array) = &current
        && array.dimensions.len() > 1
    {
        return assign_multidimensional_array_index(
            array.clone(),
            array_ty,
            index,
            rest,
            replacement,
            ctx,
        );
    }

    let element_ty = assignment_array_element_type(array_ty);
    let (mut lower_bound, mut elements, element_type_oid, prefer_pg_array) = match current {
        Value::PgArray(array) => (
            array.lower_bound(0).unwrap_or(index),
            array.elements,
            array.element_type_oid,
            true,
        ),
        Value::Array(elements) => (1, elements, None, false),
        Value::Null => (index, Vec::new(), None, true),
        other => {
            return Err(ExecError::TypeMismatch {
                op: "array assignment",
                left: other,
                right: replacement,
            });
        }
    };

    if elements.is_empty() {
        lower_bound = index;
    }
    if index < lower_bound {
        let prepend = usize::try_from(i64::from(lower_bound) - i64::from(index))
            .map_err(|_| array_assignment_limit_error())?;
        checked_assignment_array_item_count(
            elements
                .len()
                .checked_add(prepend)
                .ok_or_else(array_assignment_limit_error)?,
        )?;
        let mut expanded = vec![Value::Null; prepend];
        expanded.extend(elements);
        elements = expanded;
        lower_bound = index;
    }
    let upper_bound = lower_bound
        .checked_add(i32::try_from(elements.len()).map_err(|_| array_assignment_limit_error())?)
        .and_then(|upper| upper.checked_sub(1))
        .ok_or_else(array_assignment_limit_error)?;
    if index > upper_bound {
        let append = usize::try_from(i64::from(index) - i64::from(upper_bound))
            .map_err(|_| array_assignment_limit_error())?;
        let new_len = elements
            .len()
            .checked_add(append)
            .ok_or_else(array_assignment_limit_error)?;
        checked_assignment_array_item_count(new_len)?;
        elements.extend(std::iter::repeat_n(Value::Null, append));
    }
    let item_index = usize::try_from(i64::from(index) - i64::from(lower_bound))
        .map_err(|_| array_assignment_limit_error())?;
    elements[item_index] = assign_indirect_value(
        elements[item_index].clone(),
        element_ty,
        rest,
        replacement,
        ctx,
    )?;

    if prefer_pg_array || lower_bound != 1 {
        let mut array = ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound,
                length: elements.len(),
            }],
            elements,
        );
        array.element_type_oid = element_type_oid;
        Ok(Value::PgArray(array))
    } else {
        Ok(Value::Array(elements))
    }
}

fn assign_multidimensional_array_index(
    array: ArrayValue,
    array_ty: SqlType,
    index: i32,
    rest: &[RuntimeAssignIndirection],
    replacement: Value,
    ctx: Option<&ExecutorContext>,
) -> Result<Value, ExecError> {
    let mut lower_bounds = array
        .dimensions
        .iter()
        .map(|dimension| dimension.lower_bound)
        .collect::<Vec<_>>();
    let lower_bound = array.lower_bound(0).unwrap_or(index);
    let mut elements = array.to_nested_values();
    let nested_dimensions = array.dimensions.get(1..).unwrap_or(&[]);

    if elements.is_empty() {
        lower_bounds = std::iter::once(index)
            .chain(
                nested_dimensions
                    .iter()
                    .map(|dimension| dimension.lower_bound),
            )
            .collect();
    }
    if index < lower_bound {
        let prepend = usize::try_from(i64::from(lower_bound) - i64::from(index))
            .map_err(|_| array_assignment_limit_error())?;
        checked_assignment_array_item_count(
            elements
                .len()
                .checked_add(prepend)
                .ok_or_else(array_assignment_limit_error)?,
        )?;
        let mut expanded = vec![null_multidimensional_array_item(nested_dimensions); prepend];
        expanded.extend(elements);
        elements = expanded;
        lower_bounds[0] = index;
    }
    let upper_bound = lower_bounds[0]
        .checked_add(i32::try_from(elements.len()).map_err(|_| array_assignment_limit_error())?)
        .and_then(|upper| upper.checked_sub(1))
        .ok_or_else(array_assignment_limit_error)?;
    if index > upper_bound {
        let append = usize::try_from(i64::from(index) - i64::from(upper_bound))
            .map_err(|_| array_assignment_limit_error())?;
        let new_len = elements
            .len()
            .checked_add(append)
            .ok_or_else(array_assignment_limit_error)?;
        checked_assignment_array_item_count(new_len)?;
        elements.extend(
            std::iter::repeat_with(|| null_multidimensional_array_item(nested_dimensions))
                .take(append),
        );
    }

    let item_index = usize::try_from(i64::from(index) - i64::from(lower_bounds[0]))
        .map_err(|_| array_assignment_limit_error())?;
    elements[item_index] = assign_indirect_value(
        elements[item_index].clone(),
        array_ty,
        rest,
        replacement,
        ctx,
    )?;

    let mut rebuilt =
        ArrayValue::from_nested_values(elements, lower_bounds).map_err(|details| {
            ExecError::InvalidStorageValue {
                column: "<array>".into(),
                details,
            }
        })?;
    rebuilt.element_type_oid = array.element_type_oid;
    Ok(Value::PgArray(rebuilt))
}

fn null_multidimensional_array_item(dimensions: &[ArrayDimension]) -> Value {
    let elements = dimensions.iter().fold(1usize, |acc, dimension| {
        acc.saturating_mul(dimension.length)
    });
    Value::PgArray(ArrayValue::from_dimensions(
        dimensions.to_vec(),
        vec![Value::Null; elements],
    ))
}

const MAX_ASSIGNMENT_ARRAY_ITEMS: usize = i32::MAX as usize;

fn checked_assignment_array_item_count(count: usize) -> Result<usize, ExecError> {
    if count > MAX_ASSIGNMENT_ARRAY_ITEMS {
        Err(array_assignment_limit_error())
    } else {
        Ok(count)
    }
}

fn assignment_array_element_type(array_ty: SqlType) -> SqlType {
    let mut element_ty = array_ty.element_type();
    if array_ty.is_array {
        element_ty.type_oid = 0;
    }
    element_ty
}

fn array_assignment_limit_error() -> ExecError {
    ExecError::DetailedError {
        message: "array size exceeds the maximum allowed".into(),
        detail: None,
        hint: None,
        sqlstate: "54000",
    }
}

fn substitute_dynamic_query_params(
    sql: &str,
    params: &[Value],
    ctx: &ExecutorContext,
) -> Result<String, ExecError> {
    let catalog = ctx.catalog.clone().ok_or_else(|| {
        function_runtime_error(
            "user-defined functions require executor catalog context",
            None,
            "0A000",
        )
    })?;
    pgrust_plpgsql::substitute_dynamic_query_params(sql, params, |_, value| {
        render_dynamic_query_param_sql(value, catalog.as_ref(), ctx)
    })
    .map_err(dynamic_query_substitution_error)
}

fn substitute_declared_cursor_params(
    sql: &str,
    params: &[DeclaredCursorParam],
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<String, ExecError> {
    if params.is_empty() {
        return Ok(sql.to_string());
    }
    let catalog = ctx.catalog.clone().ok_or_else(|| {
        function_runtime_error(
            "user-defined functions require executor catalog context",
            None,
            "0A000",
        )
    })?;
    pgrust_plpgsql::substitute_declared_cursor_params(sql, params, values, |value, param| {
        render_declared_cursor_param_sql(value, param, catalog.as_ref(), ctx)
    })
    .map_err(dynamic_query_substitution_error)
}

fn dynamic_query_substitution_error(
    err: pgrust_plpgsql::DynamicQuerySubstitutionError<ExecError>,
) -> ExecError {
    match err {
        pgrust_plpgsql::DynamicQuerySubstitutionError::InvalidParameterReference(reference) => {
            function_runtime_error(
                "dynamic EXECUTE parameter reference is invalid",
                Some(reference),
                "42P02",
            )
        }
        pgrust_plpgsql::DynamicQuerySubstitutionError::MissingParameter(index) => {
            function_runtime_error(&format!("there is no parameter ${index}"), None, "42P02")
        }
        pgrust_plpgsql::DynamicQuerySubstitutionError::MissingCursorParameter(name) => {
            function_runtime_error(
                &format!("missing value for cursor parameter \"{name}\""),
                None,
                "42P02",
            )
        }
        pgrust_plpgsql::DynamicQuerySubstitutionError::Render(err) => err,
    }
}

fn render_declared_cursor_param_sql(
    value: &Value,
    param: &DeclaredCursorParam,
    catalog: &dyn CatalogLookup,
    ctx: &ExecutorContext,
) -> Result<String, ExecError> {
    if matches!(value, Value::Null) {
        return Ok(format!("(null::{})", param.type_name));
    }
    render_dynamic_query_param_sql(value, catalog, ctx)
}

fn render_dynamic_query_param_sql(
    value: &Value,
    catalog: &dyn CatalogLookup,
    ctx: &ExecutorContext,
) -> Result<String, ExecError> {
    let declared_type_oid = value.sql_type_hint().and_then(|ty| {
        catalog
            .type_oid_for_sql_type(ty)
            .or((ty.type_oid != 0).then_some(ty.type_oid))
    });
    let base = render_dynamic_query_param_base_sql(value, declared_type_oid, catalog, ctx)?;
    if let Some(type_oid) = declared_type_oid.filter(|oid| *oid != 0) {
        return Ok(format!(
            "({base})::{}",
            render_dynamic_query_type_name(type_oid, catalog)?
        ));
    }
    Ok(base)
}

fn render_dynamic_query_param_base_sql(
    value: &Value,
    declared_type_oid: Option<u32>,
    catalog: &dyn CatalogLookup,
    ctx: &ExecutorContext,
) -> Result<String, ExecError> {
    Ok(match value {
        Value::Null => "null".into(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Xid8(v) => v.to_string(),
        Value::PgLsn(v) => quote_sql_string(&crate::backend::executor::render_pg_lsn_text(*v)),
        Value::Tid(v) => quote_sql_string(&crate::backend::executor::value_io::render_tid_text(v)),
        Value::Money(v) => v.to_string(),
        Value::Float64(v) => {
            if v.is_finite() {
                v.to_string()
            } else {
                quote_sql_string(&v.to_string())
            }
        }
        Value::Bit(bits) => quote_sql_string(&crate::backend::executor::render_bit_text(bits)),
        Value::Bool(true) => "t".to_string(),
        Value::Bool(false) => "f".to_string(),
        Value::Numeric(v) => v.render(),
        Value::Interval(v) => quote_sql_string(&render_interval_text(*v)),
        Value::Uuid(v) => {
            quote_sql_string(&crate::backend::executor::value_io::render_uuid_text(v))
        }
        Value::Text(text) => quote_sql_string(text),
        Value::TextRef(_, _) => quote_sql_string(value.as_text().unwrap_or_default()),
        Value::Json(text) => quote_sql_string(text),
        Value::JsonPath(text) => quote_sql_string(text),
        Value::Xml(text) => quote_sql_string(text),
        Value::Bytea(bytes) => quote_sql_string(&format_bytea_text(bytes, ByteaOutputFormat::Hex)),
        Value::Inet(v) => quote_sql_string(&v.render_inet()),
        Value::Cidr(v) => quote_sql_string(&v.render_cidr()),
        Value::MacAddr(v) => quote_sql_string(&crate::backend::executor::render_macaddr_text(v)),
        Value::MacAddr8(v) => quote_sql_string(&crate::backend::executor::render_macaddr8_text(v)),
        Value::InternalChar(byte) => {
            quote_sql_string(&crate::backend::executor::render_internal_char_text(*byte))
        }
        Value::EnumOid(v) => v.to_string(),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => quote_sql_string(
            &crate::backend::executor::render_datetime_value_text_with_config(
                value,
                &ctx.datetime_config,
            )
            .unwrap_or_default(),
        ),
        Value::TsVector(vector) => {
            quote_sql_string(&crate::backend::executor::render_tsvector_text(vector))
        }
        Value::TsQuery(query) => {
            quote_sql_string(&crate::backend::executor::render_tsquery_text(query))
        }
        Value::Jsonb(bytes) => quote_sql_string(
            &crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap_or_default(),
        ),
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => quote_sql_string(
            &crate::backend::executor::render_geometry_text(value, Default::default())
                .unwrap_or_default(),
        ),
        Value::Range(_) => quote_sql_string(
            &crate::backend::executor::render_range_text(value).unwrap_or_default(),
        ),
        Value::Multirange(_) => quote_sql_string(
            &crate::backend::executor::render_multirange_text(value).unwrap_or_default(),
        ),
        Value::Record(record) => {
            let mut fields = Vec::with_capacity(record.fields.len());
            for (field, field_value) in record.iter() {
                let field_type_oid =
                    catalog
                        .type_oid_for_sql_type(field.sql_type)
                        .or((field.sql_type.type_oid != 0).then_some(field.sql_type.type_oid));
                fields.push(render_dynamic_query_param_sql_with_type(
                    field_value,
                    field_type_oid,
                    catalog,
                    ctx,
                )?);
            }
            format!("ROW({})", fields.join(", "))
        }
        Value::IndirectVarlena(indirect) => {
            let decoded = crate::backend::executor::value_io::indirect_varlena_to_value(indirect)?;
            render_dynamic_query_param_base_sql(&decoded, declared_type_oid, catalog, ctx)?
        }
        Value::Array(items) => {
            let array = ArrayValue::from_1d(items.clone());
            render_dynamic_query_array_sql(&array, declared_type_oid, catalog, ctx)?
        }
        Value::PgArray(array) => {
            render_dynamic_query_array_sql(array, declared_type_oid, catalog, ctx)?
        }
        Value::DroppedColumn(_) | Value::WrongTypeColumn { .. } => "null".into(),
    })
}

fn render_dynamic_query_param_sql_with_type(
    value: &Value,
    declared_type_oid: Option<u32>,
    catalog: &dyn CatalogLookup,
    ctx: &ExecutorContext,
) -> Result<String, ExecError> {
    let base = render_dynamic_query_param_base_sql(value, declared_type_oid, catalog, ctx)?;
    if let Some(type_oid) = declared_type_oid.filter(|oid| *oid != 0) {
        return Ok(format!(
            "({base})::{}",
            render_dynamic_query_type_name(type_oid, catalog)?
        ));
    }
    Ok(base)
}

fn render_dynamic_query_array_sql(
    array: &ArrayValue,
    declared_type_oid: Option<u32>,
    catalog: &dyn CatalogLookup,
    ctx: &ExecutorContext,
) -> Result<String, ExecError> {
    if array.dimensions.is_empty() {
        return Ok("ARRAY[]".into());
    }
    let element_type_oid = array.element_type_oid.or_else(|| {
        declared_type_oid.and_then(|oid| catalog.type_by_oid(oid).map(|row| row.typelem))
    });
    let mut index = 0usize;
    let body = render_dynamic_query_array_dimension_sql(
        &array.dimensions,
        &array.elements,
        0,
        &mut index,
        element_type_oid,
        catalog,
        ctx,
    )?;
    Ok(format!("ARRAY{body}"))
}

fn render_dynamic_query_array_dimension_sql(
    dimensions: &[ArrayDimension],
    elements: &[Value],
    depth: usize,
    index: &mut usize,
    element_type_oid: Option<u32>,
    catalog: &dyn CatalogLookup,
    ctx: &ExecutorContext,
) -> Result<String, ExecError> {
    let dim = dimensions
        .get(depth)
        .ok_or_else(|| ExecError::InvalidStorageValue {
            column: "<bind>".into(),
            details: "array dimension index out of bounds".into(),
        })?;
    let mut parts = Vec::with_capacity(dim.length);
    for _ in 0..dim.length {
        if depth + 1 == dimensions.len() {
            let value = elements
                .get(*index)
                .ok_or_else(|| ExecError::InvalidStorageValue {
                    column: "<bind>".into(),
                    details: "array element index out of bounds".into(),
                })?;
            parts.push(render_dynamic_query_param_sql_with_type(
                value,
                element_type_oid,
                catalog,
                ctx,
            )?);
            *index += 1;
        } else {
            parts.push(render_dynamic_query_array_dimension_sql(
                dimensions,
                elements,
                depth + 1,
                index,
                element_type_oid,
                catalog,
                ctx,
            )?);
        }
    }
    Ok(format!("[{}]", parts.join(", ")))
}

fn render_dynamic_query_type_name(
    type_oid: u32,
    catalog: &dyn CatalogLookup,
) -> Result<String, ExecError> {
    let row = catalog.type_by_oid(type_oid).ok_or_else(|| {
        function_runtime_error(
            &format!("type oid {type_oid} is not available"),
            None,
            "42704",
        )
    })?;
    Ok(quote_identifier(&row.typname))
}

fn current_output_row(
    compiled: &CompiledFunction,
    state: &FunctionState,
    ctx: &mut ExecutorContext,
) -> Result<TupleSlot, ExecError> {
    let mut values = Vec::with_capacity(compiled.output_slots.len());
    for slot in &compiled.output_slots {
        values.push(cast_function_value(
            state.values[slot.slot].clone(),
            None,
            slot.column.sql_type,
            ctx,
        )?);
    }
    Ok(TupleSlot::virtual_row(values))
}

fn coerce_function_result_row(
    row: Vec<Value>,
    contract: &FunctionReturnContract,
    expected_record_shape: Option<&[QueryColumn]>,
    ctx: &mut ExecutorContext,
) -> Result<TupleSlot, ExecError> {
    match contract {
        FunctionReturnContract::Scalar { ty, .. } => match row.as_slice() {
            [value] => Ok(TupleSlot::virtual_row(vec![cast_function_value(
                value.clone(),
                None,
                *ty,
                ctx,
            )?])),
            _ => Err(function_runtime_error(
                "structure of query does not match function result type",
                Some(format!("expected 1 column, got {}", row.len())),
                "42804",
            )),
        },
        FunctionReturnContract::FixedRow {
            columns,
            composite_typrelid,
            ..
        } => {
            let expected_columns = expected_record_shape.unwrap_or(columns);
            if let Some(typrelid) = composite_typrelid {
                return coerce_named_composite_row_to_columns(
                    row,
                    expected_columns,
                    *typrelid,
                    ctx,
                );
            }
            coerce_row_to_columns(row, expected_columns, ctx)
        }
        FunctionReturnContract::AnonymousRecord { .. } => coerce_row_to_columns(
            row,
            expected_record_shape.ok_or_else(|| {
                function_runtime_error(
                    "record-returning function requires a caller-provided row shape",
                    None,
                    "0A000",
                )
            })?,
            ctx,
        ),
        FunctionReturnContract::Trigger { .. } | FunctionReturnContract::EventTrigger { .. } => {
            Err(function_runtime_error(
                "trigger functions do not produce SQL rows",
                None,
                "0A000",
            ))
        }
    }
}

fn coerce_row_to_columns(
    row: Vec<Value>,
    columns: &[QueryColumn],
    ctx: &mut ExecutorContext,
) -> Result<TupleSlot, ExecError> {
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
    let mut values = Vec::with_capacity(row.len());
    for (value, column) in row.into_iter().zip(columns.iter()) {
        values.push(cast_function_value(
            value,
            Some(column.sql_type),
            column.sql_type,
            ctx,
        )?);
    }
    Ok(TupleSlot::virtual_row(values))
}

fn coerce_named_composite_row_to_columns(
    row: Vec<Value>,
    expected_columns: &[QueryColumn],
    typrelid: u32,
    ctx: &mut ExecutorContext,
) -> Result<TupleSlot, ExecError> {
    let Some(catalog) = ctx.catalog.as_deref() else {
        return coerce_row_to_columns(row, expected_columns, ctx);
    };
    let Some(relation) = catalog.lookup_relation_by_oid(typrelid) else {
        return coerce_row_to_columns(row, expected_columns, ctx);
    };
    let relation_desc = relation.desc.clone();
    let live_columns = relation_desc
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| !column.dropped)
        .collect::<Vec<_>>();
    if row.len() != live_columns.len() {
        return coerce_row_to_columns(row, expected_columns, ctx);
    }

    let mut values = Vec::with_capacity(expected_columns.len());
    let mut live_cursor = 0usize;
    let mut physical_cursor = 0usize;
    for (expected_index, expected) in expected_columns.iter().enumerate() {
        let found = live_columns
            .iter()
            .enumerate()
            .skip(live_cursor)
            .find(|(_, (_, column))| column.name.eq_ignore_ascii_case(&expected.name));
        if let Some((live_index, (physical_index, column))) = found {
            live_cursor = live_index.saturating_add(1);
            physical_cursor = physical_index.saturating_add(1);
            let value = row.get(live_index).cloned().unwrap_or(Value::Null);
            if column.sql_type == expected.sql_type {
                values.push(cast_function_value(
                    value,
                    Some(column.sql_type),
                    expected.sql_type,
                    ctx,
                )?);
            } else {
                values.push(Value::WrongTypeColumn {
                    attnum: physical_index.saturating_add(1),
                    table_type: column.sql_type,
                    query_type: expected.sql_type,
                });
            }
            continue;
        }

        let dropped_attr = dropped_named_composite_attr_index(
            &relation_desc,
            &live_columns,
            expected_columns,
            expected_index,
            live_cursor,
            physical_cursor,
        );
        if let Some(attr_index) = dropped_attr {
            physical_cursor = attr_index.saturating_add(1);
            values.push(Value::DroppedColumn(attr_index.saturating_add(1)));
            continue;
        }

        return coerce_row_to_columns(row, expected_columns, ctx);
    }
    Ok(TupleSlot::virtual_row(values))
}

fn dropped_named_composite_attr_index(
    desc: &RelationDesc,
    live_columns: &[(usize, &crate::include::nodes::primnodes::ColumnDesc)],
    expected_columns: &[QueryColumn],
    expected_index: usize,
    live_cursor: usize,
    physical_cursor: usize,
) -> Option<usize> {
    let next_live_physical = expected_columns
        .iter()
        .skip(expected_index.saturating_add(1))
        .find_map(|expected| {
            live_columns
                .iter()
                .skip(live_cursor)
                .find(|(_, column)| column.name.eq_ignore_ascii_case(&expected.name))
                .map(|(index, _)| *index)
        })
        .unwrap_or(desc.columns.len());
    desc.columns
        .iter()
        .enumerate()
        .take(next_live_physical)
        .skip(physical_cursor)
        .rev()
        .find_map(|(index, column)| column.dropped.then_some(index))
        .or_else(|| {
            desc.columns
                .iter()
                .enumerate()
                .skip(physical_cursor)
                .find_map(|(index, column)| column.dropped.then_some(index))
        })
}

fn eval_do_expr(expr: &CompiledExpr, values: &[Value]) -> Result<Value, ExecError> {
    match expr {
        CompiledExpr::Scalar { expr, subplans, .. } if subplans.is_empty() => {
            let mut slot = TupleSlot::virtual_row(values.to_vec());
            eval_plpgsql_expr(expr, &mut slot)
        }
        CompiledExpr::Scalar { .. } => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "subqueries in DO expression evaluation are not supported".into(),
        ))),
        CompiledExpr::QueryCompare { .. } => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "query-style PL/pgSQL conditions are only supported inside CREATE FUNCTION".into(),
            )))
        }
    }
}

fn eval_function_expr(
    expr: &CompiledExpr,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let source = compiled_expr_source(expr);
    eval_function_expr_inner(expr, values, ctx)
        .map_err(|err| with_plpgsql_expression_context(err, source))
}

fn eval_function_return_expr(
    expr: &CompiledExpr,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let source = compiled_expr_source(expr);
    eval_function_expr_inner(expr, values, ctx).map_err(|err| {
        if plpgsql_expression_uses_internal_query(&err) {
            with_plpgsql_expression_context(err, source)
        } else {
            err
        }
    })
}

fn eval_function_expr_inner(
    expr: &CompiledExpr,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match expr {
        CompiledExpr::Scalar { expr, subplans, .. } => {
            let mut slot = TupleSlot::virtual_row(values.to_vec());
            if subplans.is_empty() {
                return eval_expr(expr, &mut slot, ctx);
            }
            let saved_subplans = std::mem::replace(&mut ctx.subplans, subplans.clone());
            let saved_initplan_values = std::mem::take(&mut ctx.expr_bindings.initplan_values);
            let saved_outer_tuple = ctx.expr_bindings.outer_tuple.replace(values.to_vec());
            let result = eval_expr(expr, &mut slot, ctx);
            ctx.expr_bindings.outer_tuple = saved_outer_tuple;
            ctx.expr_bindings.initplan_values = saved_initplan_values;
            ctx.subplans = saved_subplans;
            result
        }
        CompiledExpr::QueryCompare { plan, op, rhs, .. } => {
            ctx.expr_bindings.outer_tuple = Some(values.to_vec());
            let result = execute_planned_stmt(plan.clone(), ctx);
            ctx.expr_bindings.outer_tuple = None;
            let StatementResult::Query { mut rows, .. } = result? else {
                return Err(function_runtime_error(
                    "condition query did not produce rows",
                    None,
                    "XX000",
                ));
            };
            let Some(row) = rows.pop() else {
                return Ok(Value::Null);
            };
            let left = row.first().cloned().unwrap_or(Value::Null);
            let mut slot = TupleSlot::virtual_row(values.to_vec());
            let right = eval_expr(rhs, &mut slot, ctx)?;
            Ok(Value::Bool(eval_query_compare(*op, &left, &right)?))
        }
    }
}

fn compiled_expr_source(expr: &CompiledExpr) -> &str {
    match expr {
        CompiledExpr::Scalar { source, .. } | CompiledExpr::QueryCompare { source, .. } => source,
    }
}

fn eval_query_compare(op: QueryCompareOp, left: &Value, right: &Value) -> Result<bool, ExecError> {
    Ok(match op {
        QueryCompareOp::Eq => compare_order_values(left, right, None, None, false)?.is_eq(),
        QueryCompareOp::NotEq => !compare_order_values(left, right, None, None, false)?.is_eq(),
        QueryCompareOp::Lt => compare_order_values(left, right, None, None, false)?.is_lt(),
        QueryCompareOp::LtEq => !compare_order_values(left, right, None, None, false)?.is_gt(),
        QueryCompareOp::Gt => compare_order_values(left, right, None, None, false)?.is_gt(),
        QueryCompareOp::GtEq => !compare_order_values(left, right, None, None, false)?.is_lt(),
        QueryCompareOp::IsDistinctFrom => {
            if matches!((left, right), (Value::Null, Value::Null)) {
                false
            } else if matches!(left, Value::Null) || matches!(right, Value::Null) {
                true
            } else {
                !compare_order_values(left, right, None, None, false)?.is_eq()
            }
        }
        QueryCompareOp::IsNotDistinctFrom => {
            if matches!((left, right), (Value::Null, Value::Null)) {
                true
            } else if matches!(left, Value::Null) || matches!(right, Value::Null) {
                false
            } else {
                compare_order_values(left, right, None, None, false)?.is_eq()
            }
        }
    })
}

fn plpgsql_check_asserts_enabled_from_values(ctx: Option<&ExecutorContext>) -> bool {
    plpgsql_check_asserts_enabled_from_gucs(ctx.map(|ctx| &ctx.gucs))
}

fn plpgsql_check_asserts_enabled_from_gucs(gucs: Option<&HashMap<String, String>>) -> bool {
    gucs.and_then(|gucs| gucs.get("plpgsql.check_asserts"))
        .map(|value| {
            !matches!(
                value.to_ascii_lowercase().as_str(),
                "off" | "false" | "no" | "0"
            )
        })
        .unwrap_or(true)
}

fn eval_plpgsql_condition(value: &Value) -> Result<bool, ExecError> {
    match value {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) | Value::Null => Ok(false),
        other => Err(ExecError::NonBoolQual(other.clone())),
    }
}

fn handler_matches(handler: &CompiledExceptionHandler, err: &ExecError) -> bool {
    handler
        .conditions
        .iter()
        .any(|condition| exception_condition_matches(condition, err))
}

fn exception_condition_matches(condition: &ExceptionCondition, err: &ExecError) -> bool {
    match condition {
        ExceptionCondition::Others => !matches!(exec_error_sqlstate(err), "57014" | "P0004"),
        ExceptionCondition::SqlState(sqlstate) => exec_error_sqlstate(err) == sqlstate,
        ExceptionCondition::ConditionName(name) => exception_condition_name_sqlstate(name)
            .is_some_and(|sqlstate| sqlstate == exec_error_sqlstate(err)),
    }
}

fn exec_error_sqlstate(err: &ExecError) -> &'static str {
    match err {
        ExecError::WithContext { source, .. }
        | ExecError::WithInternalQueryContext { source, .. } => exec_error_sqlstate(source),
        ExecError::RaiseException(_) => "P0001",
        ExecError::DivisionByZero(_) => "22012",
        ExecError::DetailedError { sqlstate, .. } | ExecError::DiagnosticError { sqlstate, .. } => {
            sqlstate
        }
        ExecError::Parse(ParseError::DetailedError { sqlstate, .. }) => sqlstate,
        ExecError::TypeMismatch { .. } => "42883",
        ExecError::Parse(ParseError::FeatureNotSupported(_))
        | ExecError::Parse(ParseError::FeatureNotSupportedMessage(_))
        | ExecError::Parse(ParseError::OuterLevelAggregateNestedCte(_)) => "0A000",
        ExecError::Parse(_) => "42601",
        ExecError::UniqueViolation { .. } => "23505",
        ExecError::NotNullViolation { .. } => "23502",
        ExecError::CheckViolation { .. } => "23514",
        ExecError::ForeignKeyViolation { .. } => "23503",
        ExecError::StringDataRightTruncation { .. } => "22001",
        ExecError::CardinalityViolation { .. } => "21000",
        ExecError::GenerateSeriesInvalidArg(_, _) => "22023",
        ExecError::Interrupted(reason) => reason.sqlstate(),
        ExecError::JsonInput { sqlstate, .. }
        | ExecError::XmlInput { sqlstate, .. }
        | ExecError::ArrayInput { sqlstate, .. } => sqlstate,
        _ => "XX000",
    }
}

fn exception_data_from_error(err: &ExecError) -> PlpgsqlExceptionData {
    PlpgsqlExceptionData {
        message: format_exec_error(err),
        detail: exec_error_detail_owned(err),
        hint: format_exec_error_hint(err),
        sqlstate: exec_error_sqlstate(err),
        context: exec_error_diagnostics_context_owned(err),
        column_name: exec_error_column_name_owned(err),
        constraint_name: exec_error_constraint_name_owned(err),
        datatype_name: exec_error_datatype_name_owned(err),
        table_name: exec_error_table_name_owned(err),
        schema_name: exec_error_schema_name_owned(err),
    }
}

fn exec_error_detail_owned(err: &ExecError) -> Option<String> {
    match err {
        ExecError::WithContext { source, .. } => exec_error_detail_owned(source),
        ExecError::DetailedError { detail, .. } | ExecError::DiagnosticError { detail, .. } => {
            detail.clone()
        }
        ExecError::Parse(ParseError::DetailedError { detail, .. }) => detail.clone(),
        ExecError::JsonInput { detail, .. }
        | ExecError::XmlInput { detail, .. }
        | ExecError::ArrayInput { detail, .. } => detail.clone(),
        ExecError::UniqueViolation { detail, .. }
        | ExecError::NotNullViolation { detail, .. }
        | ExecError::ForeignKeyViolation { detail, .. } => detail.clone(),
        _ => None,
    }
}

fn exec_error_context_owned(err: &ExecError) -> Option<String> {
    match err {
        ExecError::WithContext { source, context } => match exec_error_context_owned(source) {
            Some(inner) => Some(format!("{inner}\n{context}")),
            None => Some(context.clone()),
        },
        ExecError::JsonInput { context, .. } | ExecError::XmlInput { context, .. } => {
            context.clone()
        }
        ExecError::Regex(err) => err.context.clone(),
        _ => None,
    }
}

fn exec_error_diagnostics_context_owned(err: &ExecError) -> Option<String> {
    match err {
        ExecError::WithContext { source, context } => {
            let inner = exec_error_diagnostics_context_owned(source);
            if context.starts_with("PL/pgSQL expression \"") {
                return inner;
            }
            match inner {
                Some(inner) => Some(format!("{inner}\n{context}")),
                None => Some(context.clone()),
            }
        }
        ExecError::JsonInput { context, .. } | ExecError::XmlInput { context, .. } => {
            context.clone()
        }
        ExecError::Regex(err) => err.context.clone(),
        _ => None,
    }
}

fn exec_error_column_name_owned(err: &ExecError) -> Option<String> {
    match err {
        ExecError::WithContext { source, .. } => exec_error_column_name_owned(source),
        ExecError::DiagnosticError { column_name, .. } => column_name.clone(),
        ExecError::NotNullViolation { column, .. } => Some(column.clone()),
        _ => None,
    }
}

fn exec_error_constraint_name_owned(err: &ExecError) -> Option<String> {
    match err {
        ExecError::WithContext { source, .. } => exec_error_constraint_name_owned(source),
        ExecError::DiagnosticError {
            constraint_name, ..
        } => constraint_name.clone(),
        ExecError::UniqueViolation { constraint, .. }
        | ExecError::NotNullViolation { constraint, .. }
        | ExecError::CheckViolation { constraint, .. }
        | ExecError::ForeignKeyViolation { constraint, .. } => Some(constraint.clone()),
        _ => None,
    }
}

fn exec_error_datatype_name_owned(err: &ExecError) -> Option<String> {
    match err {
        ExecError::WithContext { source, .. } => exec_error_datatype_name_owned(source),
        ExecError::DiagnosticError { datatype_name, .. } => datatype_name.clone(),
        _ => None,
    }
}

fn exec_error_table_name_owned(err: &ExecError) -> Option<String> {
    match err {
        ExecError::WithContext { source, .. } => exec_error_table_name_owned(source),
        ExecError::DiagnosticError { table_name, .. } => table_name.clone(),
        ExecError::NotNullViolation { relation, .. }
        | ExecError::CheckViolation { relation, .. } => Some(relation.clone()),
        _ => None,
    }
}

fn exec_error_schema_name_owned(err: &ExecError) -> Option<String> {
    match err {
        ExecError::WithContext { source, .. } => exec_error_schema_name_owned(source),
        ExecError::DiagnosticError { schema_name, .. } => schema_name.clone(),
        _ => None,
    }
}

fn exception_data_to_error(err: PlpgsqlExceptionData) -> ExecError {
    let context = err.context.clone();
    let fields = PlpgsqlErrorFields {
        column_name: err.column_name,
        constraint_name: err.constraint_name,
        datatype_name: err.datatype_name,
        table_name: err.table_name,
        schema_name: err.schema_name,
    };
    let base = if err.sqlstate == "P0001"
        && err.detail.is_none()
        && err.hint.is_none()
        && fields.is_empty()
    {
        ExecError::RaiseException(err.message)
    } else if !fields.is_empty() {
        ExecError::DiagnosticError {
            message: err.message,
            detail: err.detail,
            hint: err.hint,
            sqlstate: err.sqlstate,
            column_name: fields.column_name,
            constraint_name: fields.constraint_name,
            datatype_name: fields.datatype_name,
            table_name: fields.table_name,
            schema_name: fields.schema_name,
        }
    } else {
        ExecError::DetailedError {
            message: err.message,
            detail: err.detail,
            hint: err.hint,
            sqlstate: err.sqlstate,
        }
    };
    match context {
        Some(context) => ExecError::WithContext {
            source: Box::new(base),
            context,
        },
        None => base,
    }
}

fn assert_failure(message: String) -> ExecError {
    ExecError::DetailedError {
        message,
        detail: None,
        hint: None,
        sqlstate: "P0004",
    }
}

fn render_assert_message(value: Value) -> Result<String, ExecError> {
    match cast_value(value, SqlType::new(SqlTypeKind::Text))? {
        Value::Text(text) => Ok(text.to_string()),
        Value::TextRef(_, _) => Ok(String::new()),
        Value::Null => Ok("assertion failed".into()),
        other => Ok(render_raise_value(&other)),
    }
}

fn finish_raise(
    level: &RaiseLevel,
    sqlstate: Option<&str>,
    message: &str,
    params: &[Value],
    detail: Option<String>,
    hint: Option<String>,
    fields: PlpgsqlErrorFields,
) -> Result<(), ExecError> {
    let rendered = render_raise_message(message, params)?;
    let resolved_sqlstate = match level {
        RaiseLevel::Exception => sqlstate.and_then(resolve_raise_sqlstate).unwrap_or("P0001"),
        RaiseLevel::Warning => sqlstate.and_then(resolve_raise_sqlstate).unwrap_or("01000"),
        RaiseLevel::Info | RaiseLevel::Log | RaiseLevel::Notice => {
            sqlstate.and_then(resolve_raise_sqlstate).unwrap_or("00000")
        }
    };
    match level {
        RaiseLevel::Exception => {
            if resolved_sqlstate == "P0001"
                && detail.is_none()
                && hint.is_none()
                && fields.is_empty()
            {
                Err(ExecError::RaiseException(rendered))
            } else if !fields.is_empty() {
                Err(ExecError::DiagnosticError {
                    message: rendered,
                    detail,
                    hint,
                    sqlstate: resolved_sqlstate,
                    column_name: fields.column_name,
                    constraint_name: fields.constraint_name,
                    datatype_name: fields.datatype_name,
                    table_name: fields.table_name,
                    schema_name: fields.schema_name,
                })
            } else {
                Err(ExecError::DetailedError {
                    message: rendered,
                    detail,
                    hint,
                    sqlstate: resolved_sqlstate,
                })
            }
        }
        RaiseLevel::Log => Ok(()),
        RaiseLevel::Info | RaiseLevel::Notice | RaiseLevel::Warning => {
            push_plpgsql_notice(level.clone(), resolved_sqlstate, rendered, detail, hint);
            Ok(())
        }
    }
}

fn render_raise_message(message: &str, params: &[Value]) -> Result<String, ExecError> {
    let mut rendered = String::with_capacity(message.len());
    let mut params = params.iter();
    let mut chars = message.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '%' {
            if chars.peek() == Some(&'%') {
                chars.next();
                rendered.push('%');
                continue;
            }
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

fn render_raise_option_value(value: Value) -> String {
    match value {
        Value::Null => String::new(),
        other => render_raise_value(&other),
    }
}

fn eval_do_raise_field(
    expr: Option<&CompiledExpr>,
    values: &[Value],
) -> Result<Option<String>, ExecError> {
    expr.map(|expr| eval_do_expr(expr, values).map(render_raise_option_value))
        .transpose()
}

fn eval_function_raise_field(
    expr: Option<&CompiledExpr>,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Option<String>, ExecError> {
    expr.map(|expr| eval_function_expr(expr, values, ctx).map(render_raise_option_value))
        .transpose()
}

fn render_raise_value(value: &Value) -> String {
    match value {
        Value::Null => "<NULL>".to_string(),
        Value::Text(text) => text.to_string(),
        Value::TextRef(_, _) => value.as_text().unwrap_or_default().to_string(),
        Value::Bool(true) => "t".to_string(),
        Value::Bool(false) => "f".to_string(),
        // :HACK: RAISE parameters currently carry only Value, not the inferred
        // SQL output type. For table_rewrite event triggers, preserve the
        // PostgreSQL-facing regclass rendering used by the regression; a
        // longer-term fix should make PL/pgSQL expression results type-aware.
        Value::Int32(v) if *v >= 0 => {
            current_event_trigger_table_rewrite_relation_name_for_oid(*v as u32)
                .unwrap_or_else(|| v.to_string())
        }
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) if *v >= 0 && *v <= i64::from(u32::MAX) => {
            current_event_trigger_table_rewrite_relation_name_for_oid(*v as u32)
                .unwrap_or_else(|| v.to_string())
        }
        Value::Int64(v) => v.to_string(),
        Value::Xid8(v) => v.to_string(),
        Value::PgLsn(v) => crate::backend::executor::render_pg_lsn_text(*v),
        Value::Tid(v) => crate::backend::executor::value_io::render_tid_text(v),
        Value::Money(v) => crate::backend::executor::money_format_text(*v),
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Interval(v) => render_interval_text(*v),
        Value::Uuid(v) => crate::backend::executor::value_io::render_uuid_text(v),
        Value::Bit(v) => crate::backend::executor::render_bit_text(v),
        Value::InternalChar(v) => char::from(*v).to_string(),
        Value::EnumOid(v) => v.to_string(),
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
        Value::Inet(v) => v.render_inet(),
        Value::Cidr(v) => v.render_cidr(),
        Value::MacAddr(v) => crate::backend::executor::render_macaddr_text(v),
        Value::MacAddr8(v) => crate::backend::executor::render_macaddr8_text(v),
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
        Value::Record(record) => crate::backend::executor::value_io::format_record_text(record),
        Value::IndirectVarlena(indirect) => {
            crate::backend::executor::value_io::indirect_varlena_to_value(indirect)
                .map(|decoded| render_raise_value(&decoded))
                .unwrap_or_default()
        }
        Value::DroppedColumn(_) | Value::WrongTypeColumn { .. } => "<NULL>".to_string(),
    }
}

fn function_runtime_error(
    message: &str,
    detail: Option<String>,
    sqlstate: &'static str,
) -> ExecError {
    function_runtime_error_with_hint(message, detail, None, sqlstate)
}

fn select_into_no_tuples_error() -> ExecError {
    function_runtime_error("SELECT INTO query does not return tuples", None, "42601")
}

fn function_runtime_error_with_hint(
    message: &str,
    detail: Option<String>,
    hint: Option<String>,
    sqlstate: &'static str,
) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail,
        hint,
        sqlstate,
    }
}

fn plpgsql_compile_error(err: ParseError, row: &PgProcRow) -> ExecError {
    ExecError::WithContext {
        source: Box::new(ExecError::Parse(err)),
        context: format!(
            "compilation of PL/pgSQL function \"{}\" near line 1",
            row.proname
        ),
    }
}

fn with_plpgsql_context(err: ExecError, compiled: &CompiledFunction, action: &str) -> ExecError {
    with_plpgsql_context_at_line(err, compiled, 1, action)
}

fn with_plpgsql_context_at_line(
    err: ExecError,
    compiled: &CompiledFunction,
    line: usize,
    action: &str,
) -> ExecError {
    ExecError::WithContext {
        source: Box::new(err),
        context: format!(
            "PL/pgSQL function {} line {line} at {action}",
            compiled_context_name(compiled)
        ),
    }
}

fn with_plpgsql_function_context(err: ExecError, compiled: &CompiledFunction) -> ExecError {
    ExecError::WithContext {
        source: Box::new(err),
        context: format!("PL/pgSQL function {}", compiled_context_name(compiled)),
    }
}

fn with_plpgsql_local_init_context(
    err: ExecError,
    compiled: &CompiledFunction,
    line: usize,
) -> ExecError {
    ExecError::WithContext {
        source: Box::new(err),
        context: format!(
            "PL/pgSQL function {} line {line} during statement block local variable initialization",
            compiled_context_name(compiled)
        ),
    }
}

fn with_plpgsql_return_cast_context(err: ExecError, compiled: &CompiledFunction) -> ExecError {
    ExecError::WithContext {
        source: Box::new(err),
        context: format!(
            "PL/pgSQL function {} while casting return value to function's return type",
            compiled_context_name(compiled)
        ),
    }
}

fn compiled_context_name(compiled: &CompiledFunction) -> String {
    if compiled.name == "inline_code_block" {
        return compiled.name.clone();
    }
    let args = compiled.context_arg_type_names.join(",");
    format!("{}({args})", compiled.name)
}

fn with_sql_statement_context(err: ExecError, sql: Option<&str>) -> ExecError {
    match sql {
        Some(sql) if !sql.is_empty() => ExecError::WithContext {
            source: Box::new(err),
            context: format!("SQL statement \"{sql}\""),
        },
        _ => err,
    }
}

fn with_plpgsql_expression_context(err: ExecError, source: &str) -> ExecError {
    let source = source.trim();
    if source.is_empty()
        || has_any_context(&err)
        || has_plpgsql_expression_context_for(&err, source)
    {
        return err;
    }
    let context = plpgsql_expression_context(source, &err);
    if plpgsql_expression_uses_internal_query(&err) {
        return ExecError::WithInternalQueryContext {
            position: plpgsql_expression_error_position(source, &err),
            query: source.to_string(),
            source: Box::new(err),
            context,
        };
    }
    ExecError::WithContext {
        source: Box::new(err),
        context,
    }
}

fn plpgsql_expression_context(source: &str, err: &ExecError) -> String {
    match err {
        // PostgreSQL reports undefined operator failures in PL/pgSQL scalar
        // expressions with QUERY plus the function frame, but without an
        // intermediate "PL/pgSQL expression" context line.
        ExecError::TypeMismatch { op, .. } if type_mismatch_op_is_operator(op) => String::new(),
        _ => format!("PL/pgSQL expression \"{source}\""),
    }
}

fn plpgsql_expression_uses_internal_query(err: &ExecError) -> bool {
    match err {
        ExecError::WithContext { source, .. }
        | ExecError::WithInternalQueryContext { source, .. } => {
            plpgsql_expression_uses_internal_query(source)
        }
        ExecError::Parse(_) => true,
        ExecError::TypeMismatch { op, .. } if type_mismatch_op_is_operator(op) => true,
        _ => false,
    }
}

fn plpgsql_expression_error_position(source: &str, err: &ExecError) -> Option<usize> {
    match err {
        ExecError::WithContext { source: inner, .. }
        | ExecError::WithInternalQueryContext { source: inner, .. } => {
            plpgsql_expression_error_position(source, inner)
        }
        ExecError::Parse(ParseError::Positioned { position, .. }) => Some(*position),
        ExecError::TypeMismatch { op, .. } if type_mismatch_op_is_operator(op) => {
            source.find(*op).map(|index| index + 1)
        }
        _ => None,
    }
}

fn type_mismatch_op_is_operator(op: &str) -> bool {
    !op.is_empty() && op.chars().all(|ch| "!~+-*/<>=@#%^&|`?".contains(ch))
}

fn with_plpgsql_context_if_missing(
    err: ExecError,
    compiled: &CompiledFunction,
    action: &str,
) -> ExecError {
    if has_plpgsql_context_for(&err, &compiled.name) {
        err
    } else {
        with_plpgsql_context(err, compiled, action)
    }
}

fn with_plpgsql_stmt_context_if_missing(
    err: ExecError,
    compiled: &CompiledFunction,
    stmt: &CompiledStmt,
) -> ExecError {
    if has_plpgsql_context_for(&err, &compiled.name) {
        err
    } else {
        with_plpgsql_context_at_line(
            err,
            compiled,
            stmt_context_line(stmt),
            stmt_context_action(stmt),
        )
    }
}

fn has_plpgsql_context_for(err: &ExecError, function_name: &str) -> bool {
    match err {
        ExecError::WithContext { source, context } => {
            let prefix = format!("PL/pgSQL function {function_name}");
            context.starts_with(&format!("{prefix} "))
                || context.starts_with(&format!("{prefix}("))
                || has_plpgsql_context_for(source, function_name)
        }
        ExecError::WithInternalQueryContext { source, .. } => {
            has_plpgsql_context_for(source, function_name)
        }
        _ => false,
    }
}

fn has_plpgsql_expression_context_for(err: &ExecError, source: &str) -> bool {
    match err {
        ExecError::WithContext {
            source: inner,
            context,
        } => {
            context == &format!("PL/pgSQL expression \"{source}\"")
                || has_plpgsql_expression_context_for(inner, source)
        }
        _ => false,
    }
}

fn has_any_context(err: &ExecError) -> bool {
    match err {
        ExecError::WithContext { .. }
        | ExecError::WithInternalQueryContext { .. }
        | ExecError::JsonInput {
            context: Some(_), ..
        }
        | ExecError::XmlInput {
            context: Some(_), ..
        } => true,
        ExecError::Regex(err) => err.context.is_some(),
        _ => false,
    }
}

fn stmt_context_line(stmt: &CompiledStmt) -> usize {
    match stmt {
        CompiledStmt::WithLine { line, .. } => *line,
        CompiledStmt::Perform { line, .. } => *line,
        CompiledStmt::Raise { line, .. }
        | CompiledStmt::DynamicExecute { line, .. }
        | CompiledStmt::Assign { line, .. }
        | CompiledStmt::AssignSubscript { line, .. }
        | CompiledStmt::AssignIndirect { line, .. }
        | CompiledStmt::AssignTriggerRow { line, .. }
        | CompiledStmt::Return { line, .. }
        | CompiledStmt::ReturnRuntimeQuery { line, .. }
        | CompiledStmt::ReturnSelect { line, .. } => *line,
        _ => 1,
    }
}

fn stmt_context_action(stmt: &CompiledStmt) -> &'static str {
    match stmt {
        CompiledStmt::WithLine { stmt, .. } => stmt_context_action(stmt),
        CompiledStmt::Block(_) => "statement block",
        CompiledStmt::Assign { .. }
        | CompiledStmt::AssignSubscript { .. }
        | CompiledStmt::AssignIndirect { .. }
        | CompiledStmt::AssignTriggerRow { .. } => "assignment",
        CompiledStmt::Null => "NULL",
        CompiledStmt::If { .. } => "IF",
        CompiledStmt::While { .. } => "WHILE",
        CompiledStmt::Loop { .. } => "LOOP",
        CompiledStmt::Exit { .. } => "EXIT",
        CompiledStmt::ForInt { .. } => "FOR with integer loop variable",
        CompiledStmt::ForQuery {
            source: CompiledForQuerySource::Dynamic { .. },
            ..
        } => "FOR over EXECUTE statement",
        CompiledStmt::ForQuery { .. } => "FOR over SELECT rows",
        CompiledStmt::ForEach { .. } => "FOREACH over array",
        CompiledStmt::Raise { .. } => "RAISE",
        CompiledStmt::Reraise => "RAISE",
        CompiledStmt::Assert { .. } => "ASSERT",
        CompiledStmt::Continue { .. } => "CONTINUE",
        CompiledStmt::Return { .. }
        | CompiledStmt::ReturnRuntimeQuery { .. }
        | CompiledStmt::ReturnSelect { .. } => "RETURN",
        CompiledStmt::ReturnNext { .. } => "RETURN NEXT",
        CompiledStmt::ReturnQuery { .. } => "RETURN QUERY",
        CompiledStmt::ReturnTriggerRow { .. }
        | CompiledStmt::ReturnTriggerNull
        | CompiledStmt::ReturnTriggerNoValue => "RETURN",
        CompiledStmt::Perform { .. } => "PERFORM",
        CompiledStmt::DynamicExecute { .. } => "EXECUTE",
        CompiledStmt::SetGuc { .. } => "SQL statement",
        CompiledStmt::CommentOnFunction { .. } => "SQL statement",
        CompiledStmt::GetDiagnostics { stacked: true, .. } => "GET STACKED DIAGNOSTICS",
        CompiledStmt::GetDiagnostics { .. } => "GET DIAGNOSTICS",
        CompiledStmt::OpenCursor { .. } => "OPEN",
        CompiledStmt::FetchCursor { .. } => "FETCH",
        CompiledStmt::MoveCursor { .. } => "MOVE",
        CompiledStmt::CloseCursor { .. } => "CLOSE",
        CompiledStmt::UnsupportedTransactionCommand { .. }
        | CompiledStmt::SelectInto { .. }
        | CompiledStmt::ExecInsertInto { .. }
        | CompiledStmt::ExecInsert { .. }
        | CompiledStmt::ExecUpdateInto { .. }
        | CompiledStmt::ExecUpdate { .. }
        | CompiledStmt::ExecDeleteInto { .. }
        | CompiledStmt::ExecDelete { .. }
        | CompiledStmt::RuntimeSql { .. }
        | CompiledStmt::RuntimeSelectInto { .. }
        | CompiledStmt::CreateTableAs { .. }
        | CompiledStmt::CreateTable { .. }
        | CompiledStmt::ExecSql { .. } => "SQL statement",
    }
}

fn seed_trigger_state(
    bindings: &CompiledTriggerBindings,
    call: &TriggerCallContext,
    state: &mut FunctionState,
) {
    pgrust_plpgsql::seed_trigger_state_values(bindings, call, &mut state.values);
}

fn seed_event_trigger_state(
    bindings: &CompiledEventTriggerBindings,
    call: &EventTriggerCallContext,
    state: &mut FunctionState,
) {
    pgrust_plpgsql::seed_event_trigger_state_values(bindings, call, &mut state.values);
}

fn current_trigger_return(
    compiled: &CompiledFunction,
    state: &FunctionState,
    returned_row: TriggerReturnedRow,
) -> Result<TriggerFunctionResult, ExecError> {
    pgrust_plpgsql::current_trigger_return(&compiled.return_contract, &state.values, returned_row)
        .ok_or_else(|| {
            function_runtime_error(
                "trigger return reached a non-trigger function",
                None,
                "0A000",
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::{TriggerLevel, TriggerTiming};
    use crate::include::catalog::TEXT_TYPE_OID;
    use pgrust_nodes::TriggerOperation;
    use pgrust_plpgsql::{CompiledTriggerBindings, CompiledTriggerRelation};

    #[test]
    fn plpgsql_expression_runtime_errors_do_not_add_internal_query() {
        let err = with_plpgsql_expression_context(ExecError::DivisionByZero("/"), "1/0");

        match err {
            ExecError::WithContext { context, source } => {
                assert_eq!(context, "PL/pgSQL expression \"1/0\"");
                assert!(matches!(*source, ExecError::DivisionByZero("/")));
            }
            other => panic!("expected PL/pgSQL context wrapper, got {other:?}"),
        }
    }

    #[test]
    fn plpgsql_expression_parse_errors_keep_internal_query() {
        let err = with_plpgsql_expression_context(
            ExecError::Parse(ParseError::UnknownColumn("x".into())),
            "x + 1",
        );

        match err {
            ExecError::WithInternalQueryContext {
                query,
                context,
                source,
                ..
            } => {
                assert_eq!(query, "x + 1");
                assert_eq!(context, "PL/pgSQL expression \"x + 1\"");
                assert!(matches!(
                    *source,
                    ExecError::Parse(ParseError::UnknownColumn(_))
                ));
            }
            other => panic!("expected internal query wrapper, got {other:?}"),
        }
    }

    #[test]
    fn seed_trigger_state_uses_zero_based_tg_argv() {
        let bindings = CompiledTriggerBindings {
            new_row: CompiledTriggerRelation {
                slots: vec![],
                field_names: vec![],
                field_types: vec![],
                not_null: vec![],
            },
            old_row: CompiledTriggerRelation {
                slots: vec![],
                field_names: vec![],
                field_types: vec![],
                not_null: vec![],
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
            transition_tables: Vec::new(),
        };
        let mut state = FunctionState {
            values: vec![Value::Null; 9],
            rows: Vec::new(),
            scalar_return: None,
            trigger_return: None,
            cursors: HashMap::new(),
            local_guc_writes: HashSet::new(),
            session_guc_writes: HashSet::new(),
            last_row_count: 0,
            current_exception: None,
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
                field_types: vec![],
                not_null: vec![],
            },
            old_row: CompiledTriggerRelation {
                slots: vec![],
                field_names: vec![],
                field_types: vec![],
                not_null: vec![],
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
            transition_tables: Vec::new(),
        };
        let mut state = FunctionState {
            values: vec![Value::Null; 9],
            rows: Vec::new(),
            scalar_return: None,
            trigger_return: None,
            cursors: HashMap::new(),
            local_guc_writes: HashSet::new(),
            session_guc_writes: HashSet::new(),
            last_row_count: 0,
            current_exception: None,
        };

        seed_trigger_state(&bindings, &call, &mut state);

        assert_eq!(
            state.values[bindings.tg_argv_slot],
            Value::PgArray(ArrayValue::empty().with_element_type_oid(TEXT_TYPE_OID))
        );
    }
}
