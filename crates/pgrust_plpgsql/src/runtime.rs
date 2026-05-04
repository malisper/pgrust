use pgrust_analyze::{BoundRelation, CatalogLookup};
use pgrust_catalog_data::PgProcRow;
use pgrust_catalog_data::bootstrap::TEXT_TYPE_OID;
use pgrust_nodes::datum::{ArrayDimension, ArrayValue, RecordValue};
use pgrust_nodes::parsenodes::DropTableStatement;
use pgrust_nodes::primnodes::{
    Expr, OpExprKind, QueryColumn, SELF_ITEM_POINTER_ATTR_NO, TABLE_OID_ATTR_NO, Var,
};
use pgrust_nodes::{
    EventTriggerCallContext, EventTriggerDdlCommandRow, EventTriggerDroppedObjectRow, SqlType,
    SqlTypeKind, StatementResult, SystemVarBinding, TriggerCallContext, TriggerFunctionResult,
    TriggerOperation, TriggerTransitionTable, Value,
};

use crate::{
    CompiledEventTriggerBindings, CompiledForQueryTarget, CompiledTriggerBindings,
    CompiledTriggerRelation, DeclaredCursorParam, FunctionReturnContract, TriggerReturnedRow,
    dollar_quote_tag_at, is_identifier_char, is_identifier_start,
};
use crate::{CursorDirection, PlpgsqlNotice, RaiseLevel};

#[derive(Debug, Clone)]
pub struct PlpgsqlExceptionData {
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub sqlstate: &'static str,
    pub context: Option<String>,
    pub column_name: Option<String>,
    pub constraint_name: Option<String>,
    pub datatype_name: Option<String>,
    pub table_name: Option<String>,
    pub schema_name: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct PlpgsqlErrorFields {
    pub column_name: Option<String>,
    pub constraint_name: Option<String>,
    pub datatype_name: Option<String>,
    pub table_name: Option<String>,
    pub schema_name: Option<String>,
}

impl PlpgsqlErrorFields {
    pub fn is_empty(&self) -> bool {
        self.column_name.is_none()
            && self.constraint_name.is_none()
            && self.datatype_name.is_none()
            && self.table_name.is_none()
            && self.schema_name.is_none()
    }
}

#[derive(Debug, Clone)]
pub struct PlpgsqlContextFrame {
    pub function_name: String,
    pub line: usize,
    pub action: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionControl {
    Continue,
    LoopContinue,
    Return,
    ExitLoop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoControl {
    Continue,
    LoopContinue,
}

thread_local! {
    static NOTICE_QUEUE: std::cell::RefCell<Vec<PlpgsqlNotice>> = const { std::cell::RefCell::new(Vec::new()) };
    static EVENT_TRIGGER_DDL_COMMANDS: std::cell::RefCell<Vec<Vec<EventTriggerDdlCommandRow>>> = const { std::cell::RefCell::new(Vec::new()) };
    static EVENT_TRIGGER_DROPPED_OBJECTS: std::cell::RefCell<Vec<Vec<EventTriggerDroppedObjectRow>>> = const { std::cell::RefCell::new(Vec::new()) };
    static EVENT_TRIGGER_TABLE_REWRITE: std::cell::RefCell<Vec<Option<(u32, i32, String)>>> = const { std::cell::RefCell::new(Vec::new()) };
    static CONTEXT_STACK: std::cell::RefCell<Vec<PlpgsqlContextFrame>> = const { std::cell::RefCell::new(Vec::new()) };
}

pub fn take_notices() -> Vec<PlpgsqlNotice> {
    NOTICE_QUEUE.with(|queue| std::mem::take(&mut *queue.borrow_mut()))
}

pub fn clear_notices() {
    NOTICE_QUEUE.with(|queue| queue.borrow_mut().clear());
}

pub fn push_plpgsql_notice(
    level: RaiseLevel,
    sqlstate: impl Into<String>,
    message: impl Into<String>,
    detail: Option<String>,
    hint: Option<String>,
) {
    NOTICE_QUEUE.with(|queue| {
        queue.borrow_mut().push(PlpgsqlNotice {
            level,
            sqlstate: sqlstate.into(),
            message: message.into(),
            detail,
            hint,
        })
    });
}

pub fn queue_plpgsql_warning(message: &str, detail: Option<String>, hint: Option<String>) {
    push_plpgsql_notice(RaiseLevel::Warning, "01000", message, detail, hint);
}

pub fn current_event_trigger_ddl_commands() -> Vec<EventTriggerDdlCommandRow> {
    EVENT_TRIGGER_DDL_COMMANDS.with(|stack| stack.borrow().last().cloned().unwrap_or_default())
}

pub fn current_event_trigger_dropped_objects() -> Vec<EventTriggerDroppedObjectRow> {
    EVENT_TRIGGER_DROPPED_OBJECTS.with(|stack| stack.borrow().last().cloned().unwrap_or_default())
}

pub fn current_event_trigger_table_rewrite() -> Option<(u32, i32)> {
    EVENT_TRIGGER_TABLE_REWRITE.with(|stack| {
        stack
            .borrow()
            .last()
            .and_then(|row| row.as_ref().map(|(oid, reason, _)| (*oid, *reason)))
    })
}

pub fn current_event_trigger_table_rewrite_relation_name_for_oid(oid: u32) -> Option<String> {
    EVENT_TRIGGER_TABLE_REWRITE.with(|stack| {
        stack.borrow().last().and_then(|row| {
            row.as_ref()
                .filter(|(relation_oid, _, _)| *relation_oid == oid)
                .map(|(_, _, relation_name)| relation_name.clone())
        })
    })
}

pub struct PlpgsqlContextFrameGuard;

impl Drop for PlpgsqlContextFrameGuard {
    fn drop(&mut self) {
        CONTEXT_STACK.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}

pub fn push_context_frame(frame: PlpgsqlContextFrame) -> PlpgsqlContextFrameGuard {
    CONTEXT_STACK.with(|stack| {
        stack.borrow_mut().push(frame);
    });
    PlpgsqlContextFrameGuard
}

pub fn current_plpgsql_context() -> Option<String> {
    CONTEXT_STACK.with(|stack| {
        let stack = stack.borrow();
        (!stack.is_empty()).then(|| {
            stack
                .iter()
                .rev()
                .map(|frame| {
                    format!(
                        "PL/pgSQL function {} line {} at {}",
                        frame.function_name, frame.line, frame.action
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
    })
}

pub struct EventTriggerDdlCommandGuard;

impl Drop for EventTriggerDdlCommandGuard {
    fn drop(&mut self) {
        EVENT_TRIGGER_DDL_COMMANDS.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}

pub struct EventTriggerTableRewriteGuard;

impl Drop for EventTriggerTableRewriteGuard {
    fn drop(&mut self) {
        EVENT_TRIGGER_TABLE_REWRITE.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}

pub struct EventTriggerDroppedObjectsGuard;

impl Drop for EventTriggerDroppedObjectsGuard {
    fn drop(&mut self) {
        EVENT_TRIGGER_DROPPED_OBJECTS.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}

pub fn push_event_trigger_ddl_commands(
    call: &EventTriggerCallContext,
) -> EventTriggerDdlCommandGuard {
    let rows = call.ddl_commands.clone();
    EVENT_TRIGGER_DDL_COMMANDS.with(|stack| stack.borrow_mut().push(rows));
    EventTriggerDdlCommandGuard
}

pub fn push_event_trigger_dropped_objects(
    call: &EventTriggerCallContext,
) -> EventTriggerDroppedObjectsGuard {
    let rows = if call.event.eq_ignore_ascii_case("sql_drop") {
        call.dropped_objects.clone()
    } else {
        Vec::new()
    };
    EVENT_TRIGGER_DROPPED_OBJECTS.with(|stack| stack.borrow_mut().push(rows));
    EventTriggerDroppedObjectsGuard
}

pub fn push_event_trigger_table_rewrite(
    call: &EventTriggerCallContext,
) -> EventTriggerTableRewriteGuard {
    let row = call
        .table_rewrite_relation_oid
        .zip(call.table_rewrite_reason)
        .zip(call.table_rewrite_relation_name.clone())
        .map(|((oid, reason), relation_name)| (oid, reason, relation_name))
        .filter(|_| call.event.eq_ignore_ascii_case("table_rewrite"));
    EVENT_TRIGGER_TABLE_REWRITE.with(|stack| stack.borrow_mut().push(row));
    EventTriggerTableRewriteGuard
}

pub fn transition_table_visible_rows(table: &TriggerTransitionTable) -> Vec<Vec<Value>> {
    let visible_indexes = table
        .desc
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| (!column.dropped).then_some(index))
        .collect::<Vec<_>>();
    table
        .rows
        .iter()
        .map(|row| {
            visible_indexes
                .iter()
                .map(|index| row.get(*index).cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlpgsqlRoutineValidationError {
    WrongFunctionKind {
        prokind: char,
    },
    WrongProcedureKind {
        prokind: char,
    },
    UnknownLanguage {
        prolang: u32,
    },
    UnsupportedLanguage {
        object_kind: String,
        language_name: String,
    },
}

pub fn validate_plpgsql_function_row(
    row: &PgProcRow,
    language_name: Option<&str>,
    object_kind: &str,
) -> Result<(), PlpgsqlRoutineValidationError> {
    if row.prokind != 'f' {
        return Err(PlpgsqlRoutineValidationError::WrongFunctionKind {
            prokind: row.prokind,
        });
    }
    validate_plpgsql_language(row.prolang, language_name, object_kind)
}

pub fn validate_plpgsql_procedure_row(
    row: &PgProcRow,
    language_name: Option<&str>,
) -> Result<(), PlpgsqlRoutineValidationError> {
    if row.prokind != 'p' {
        return Err(PlpgsqlRoutineValidationError::WrongProcedureKind {
            prokind: row.prokind,
        });
    }
    validate_plpgsql_language(row.prolang, language_name, "procedure")
}

fn validate_plpgsql_language(
    prolang: u32,
    language_name: Option<&str>,
    object_kind: &str,
) -> Result<(), PlpgsqlRoutineValidationError> {
    let Some(language_name) = language_name else {
        return Err(PlpgsqlRoutineValidationError::UnknownLanguage { prolang });
    };
    if !language_name.eq_ignore_ascii_case("plpgsql") {
        return Err(PlpgsqlRoutineValidationError::UnsupportedLanguage {
            object_kind: object_kind.to_string(),
            language_name: language_name.to_string(),
        });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarCallContext {
    ExprArgs,
    ValueArgs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlpgsqlScalarCallValidationError {
    SetReturningInScalarContext,
    TriggerInScalarContext { context: ScalarCallContext },
    EventTriggerInScalarContext { context: ScalarCallContext },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlpgsqlTriggerContractError {
    NonTrigger,
    NonEventTrigger,
}

pub fn validate_scalar_call_return_contract(
    contract: &FunctionReturnContract,
    context: ScalarCallContext,
) -> Result<(), PlpgsqlScalarCallValidationError> {
    match contract {
        FunctionReturnContract::Scalar { setof: true, .. }
        | FunctionReturnContract::FixedRow { setof: true, .. }
        | FunctionReturnContract::AnonymousRecord { setof: true } => {
            Err(PlpgsqlScalarCallValidationError::SetReturningInScalarContext)
        }
        FunctionReturnContract::Trigger { .. } => {
            Err(PlpgsqlScalarCallValidationError::TriggerInScalarContext { context })
        }
        FunctionReturnContract::EventTrigger { .. } => {
            Err(PlpgsqlScalarCallValidationError::EventTriggerInScalarContext { context })
        }
        FunctionReturnContract::Scalar { .. }
        | FunctionReturnContract::FixedRow { .. }
        | FunctionReturnContract::AnonymousRecord { setof: false } => Ok(()),
    }
}

pub fn trigger_return_bindings(
    contract: &FunctionReturnContract,
) -> Result<&CompiledTriggerBindings, PlpgsqlTriggerContractError> {
    match contract {
        FunctionReturnContract::Trigger { bindings } => Ok(bindings),
        _ => Err(PlpgsqlTriggerContractError::NonTrigger),
    }
}

pub fn event_trigger_return_bindings(
    contract: &FunctionReturnContract,
) -> Result<&CompiledEventTriggerBindings, PlpgsqlTriggerContractError> {
    match contract {
        FunctionReturnContract::EventTrigger { bindings } => Ok(bindings),
        _ => Err(PlpgsqlTriggerContractError::NonEventTrigger),
    }
}

pub fn event_trigger_object_type_for_tag(tag: &str) -> String {
    tag.strip_prefix("CREATE ")
        .or_else(|| tag.strip_prefix("ALTER "))
        .or_else(|| tag.strip_prefix("DROP "))
        .unwrap_or(tag)
        .to_ascii_lowercase()
}

#[cfg(test)]
mod transition_table_tests {
    use super::*;
    use pgrust_catalog_data::desc::column_desc;

    #[test]
    fn transition_table_visible_rows_drop_dropped_columns() {
        let mut dropped = column_desc("b", SqlType::new(SqlTypeKind::Int4), true);
        dropped.dropped = true;
        let table = TriggerTransitionTable {
            name: "new_table".into(),
            desc: pgrust_nodes::primnodes::RelationDesc {
                columns: vec![
                    column_desc("a", SqlType::new(SqlTypeKind::Int4), true),
                    dropped,
                    column_desc("c", SqlType::new(SqlTypeKind::Text), true),
                ],
            },
            rows: vec![vec![
                Value::Int32(1),
                Value::Int32(2),
                Value::Text("x".into()),
            ]],
        };

        assert_eq!(
            transition_table_visible_rows(&table),
            vec![vec![Value::Int32(1), Value::Text("x".into())]]
        );
    }

    #[test]
    fn routine_validation_checks_prokind_and_language() {
        let mut function = proc_row('f');
        assert_eq!(
            validate_plpgsql_function_row(&function, Some("sql"), "function"),
            Err(PlpgsqlRoutineValidationError::UnsupportedLanguage {
                object_kind: "function".into(),
                language_name: "sql".into(),
            })
        );
        assert_eq!(
            validate_plpgsql_function_row(&function, None, "function"),
            Err(PlpgsqlRoutineValidationError::UnknownLanguage {
                prolang: function.prolang,
            })
        );
        function.prokind = 'p';
        assert_eq!(
            validate_plpgsql_function_row(&function, Some("plpgsql"), "function"),
            Err(PlpgsqlRoutineValidationError::WrongFunctionKind { prokind: 'p' })
        );
        assert!(validate_plpgsql_procedure_row(&function, Some("plpgsql")).is_ok());
    }

    #[test]
    fn scalar_call_validation_rejects_set_returning_contracts() {
        let contract = FunctionReturnContract::Scalar {
            ty: SqlType::new(SqlTypeKind::Int4),
            setof: true,
            output_slot: None,
        };

        assert_eq!(
            validate_scalar_call_return_contract(&contract, ScalarCallContext::ValueArgs),
            Err(PlpgsqlScalarCallValidationError::SetReturningInScalarContext)
        );
        assert_eq!(
            trigger_return_bindings(&contract).err(),
            Some(PlpgsqlTriggerContractError::NonTrigger)
        );
        assert_eq!(
            event_trigger_return_bindings(&contract).err(),
            Some(PlpgsqlTriggerContractError::NonEventTrigger)
        );
    }

    fn proc_row(prokind: char) -> PgProcRow {
        PgProcRow {
            oid: 1,
            proname: "f".into(),
            pronamespace: 0,
            proowner: 0,
            proacl: None,
            prolang: 999,
            procost: 1.0,
            prorows: 0.0,
            provariadic: 0,
            prosupport: 0,
            prokind,
            prosecdef: false,
            proleakproof: false,
            proisstrict: false,
            proretset: false,
            provolatile: 'v',
            proparallel: 'u',
            pronargs: 0,
            pronargdefaults: 0,
            prorettype: 0,
            proargtypes: String::new(),
            proallargtypes: None,
            proargmodes: None,
            proargnames: None,
            proargdefaults: None,
            prosrc: String::new(),
            probin: None,
            prosqlbody: None,
            proconfig: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FunctionCursor {
    pub columns: Vec<QueryColumn>,
    pub rows: Vec<FunctionQueryRow>,
    pub current: isize,
    pub scrollable: bool,
}

#[derive(Debug, Clone)]
pub struct FunctionQueryRow {
    pub values: Vec<Value>,
    pub system_bindings: Vec<SystemVarBinding>,
}

#[derive(Debug)]
pub struct FunctionQueryResult {
    pub columns: Vec<QueryColumn>,
    pub rows: Vec<FunctionQueryRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtraCheckLevel {
    Warning,
    Error,
}

pub fn diagnostic_text(value: Option<&str>) -> Value {
    Value::Text(value.unwrap_or_default().to_string().into())
}

pub fn plpgsql_extra_check_level(
    gucs: &std::collections::HashMap<String, String>,
    check: &str,
) -> Option<ExtraCheckLevel> {
    if plpgsql_extra_check_enabled(gucs.get("plpgsql.extra_errors"), check) {
        Some(ExtraCheckLevel::Error)
    } else if plpgsql_extra_check_enabled(gucs.get("plpgsql.extra_warnings"), check) {
        Some(ExtraCheckLevel::Warning)
    } else {
        None
    }
}

pub fn plpgsql_extra_check_enabled(value: Option<&String>, check: &str) -> bool {
    value.is_some_and(|value| {
        let value = value.trim();
        value.eq_ignore_ascii_case("all")
            || value
                .split(',')
                .any(|item| item.trim().eq_ignore_ascii_case(check))
    })
}

pub fn function_query_row_values(rows: &[FunctionQueryRow]) -> Vec<Vec<Value>> {
    rows.iter().map(|row| row.values.clone()).collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlpgsqlForeachRuntimeError {
    SliceTargetMustBeArray,
    SliceDimensionOutOfRange { slice: usize, ndim: usize },
    ArraySizeLimitExceeded,
}

impl PlpgsqlForeachRuntimeError {
    pub fn message(&self) -> String {
        match self {
            PlpgsqlForeachRuntimeError::SliceTargetMustBeArray => {
                "FOREACH ... SLICE loop variable must be of an array type".into()
            }
            PlpgsqlForeachRuntimeError::SliceDimensionOutOfRange { slice, ndim } => {
                format!("slice dimension ({slice}) is out of the valid range 0..{ndim}")
            }
            PlpgsqlForeachRuntimeError::ArraySizeLimitExceeded => {
                "array size exceeds the maximum allowed".into()
            }
        }
    }

    pub fn sqlstate(&self) -> &'static str {
        match self {
            PlpgsqlForeachRuntimeError::SliceTargetMustBeArray => "42804",
            PlpgsqlForeachRuntimeError::SliceDimensionOutOfRange { .. } => "2202E",
            PlpgsqlForeachRuntimeError::ArraySizeLimitExceeded => "54000",
        }
    }
}

pub fn validate_foreach_target(
    target: &CompiledForQueryTarget,
    slice: usize,
) -> Result<(), PlpgsqlForeachRuntimeError> {
    if slice == 0 {
        return Ok(());
    }
    if target
        .targets
        .iter()
        .any(|target| !target.ty.is_array && target.ty.kind != SqlTypeKind::Record)
    {
        return Err(PlpgsqlForeachRuntimeError::SliceTargetMustBeArray);
    }
    Ok(())
}

pub fn foreach_iteration_values(
    array: &ArrayValue,
    slice: usize,
) -> Result<Vec<Value>, PlpgsqlForeachRuntimeError> {
    if array.is_empty() {
        return Ok(Vec::new());
    }
    if slice == 0 {
        return Ok(array.elements.clone());
    }
    let ndim = array.ndim();
    if slice > ndim {
        return Err(PlpgsqlForeachRuntimeError::SliceDimensionOutOfRange { slice, ndim });
    }
    let slice_dims = array.dimensions[ndim - slice..].to_vec();
    let slice_len = slice_dims
        .iter()
        .try_fold(1usize, |acc, dim| acc.checked_mul(dim.length))
        .ok_or(PlpgsqlForeachRuntimeError::ArraySizeLimitExceeded)?;
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

pub fn statement_result_to_query_result(result: StatementResult) -> Option<FunctionQueryResult> {
    let StatementResult::Query { columns, rows, .. } = result else {
        return None;
    };
    Some(FunctionQueryResult {
        columns,
        rows: rows
            .into_iter()
            .map(|values| FunctionQueryRow {
                values,
                system_bindings: Vec::new(),
            })
            .collect(),
    })
}

pub fn statement_result_changed_rows(result: &StatementResult) -> bool {
    match result {
        StatementResult::AffectedRows(rows) => *rows > 0,
        StatementResult::Query { rows, .. } => !rows.is_empty(),
    }
}

pub fn trigger_relation_record_value(
    relation: &CompiledTriggerRelation,
    values: &[Value],
) -> Value {
    Value::Record(RecordValue::anonymous(
        relation
            .slots
            .iter()
            .zip(relation.field_names.iter())
            .map(|(slot, name)| (name.clone(), values[*slot].clone()))
            .collect(),
    ))
}

pub fn function_outer_tuple(contract: &FunctionReturnContract, values: &[Value]) -> Vec<Value> {
    let mut outer_values = values.to_vec();
    if let FunctionReturnContract::Trigger { bindings } = contract {
        outer_values.push(trigger_relation_record_value(&bindings.new_row, values));
        outer_values.push(trigger_relation_record_value(&bindings.old_row, values));
    }
    outer_values
}

pub fn current_trigger_return(
    contract: &FunctionReturnContract,
    values: &[Value],
    returned_row: TriggerReturnedRow,
) -> Option<TriggerFunctionResult> {
    let FunctionReturnContract::Trigger { bindings } = contract else {
        return None;
    };
    let relation = match returned_row {
        TriggerReturnedRow::New => &bindings.new_row,
        TriggerReturnedRow::Old => &bindings.old_row,
    };
    let row_values = relation
        .slots
        .iter()
        .map(|slot| values[*slot].clone())
        .collect::<Vec<_>>();
    Some(match returned_row {
        TriggerReturnedRow::New => TriggerFunctionResult::ReturnNew(row_values),
        TriggerReturnedRow::Old => TriggerFunctionResult::ReturnOld(row_values),
    })
}

pub fn seed_trigger_state_values(
    bindings: &CompiledTriggerBindings,
    call: &TriggerCallContext,
    values: &mut [Value],
) {
    seed_trigger_relation_values(&bindings.new_row, call.new_row.as_ref(), values);
    seed_trigger_relation_values(&bindings.old_row, call.old_row.as_ref(), values);
    values[bindings.tg_name_slot] = Value::Text(call.trigger_name.clone().into());
    values[bindings.tg_op_slot] = Value::Text(
        match call.op {
            TriggerOperation::Insert => "INSERT",
            TriggerOperation::Update => "UPDATE",
            TriggerOperation::Delete => "DELETE",
            TriggerOperation::Truncate => "TRUNCATE",
        }
        .into(),
    );
    values[bindings.tg_when_slot] = Value::Text(
        match call.timing {
            pgrust_nodes::parsenodes::TriggerTiming::Before => "BEFORE",
            pgrust_nodes::parsenodes::TriggerTiming::After => "AFTER",
            pgrust_nodes::parsenodes::TriggerTiming::Instead => "INSTEAD OF",
        }
        .into(),
    );
    values[bindings.tg_level_slot] = Value::Text(
        match call.level {
            pgrust_nodes::parsenodes::TriggerLevel::Row => "ROW",
            pgrust_nodes::parsenodes::TriggerLevel::Statement => "STATEMENT",
        }
        .into(),
    );
    values[bindings.tg_relid_slot] = Value::Int64(i64::from(call.relation_oid));
    values[bindings.tg_nargs_slot] = Value::Int32(call.trigger_args.len() as i32);
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
    values[bindings.tg_argv_slot] = Value::PgArray(tg_argv.with_element_type_oid(TEXT_TYPE_OID));
    values[bindings.tg_table_name_slot] = Value::Text(call.table_name.clone().into());
    values[bindings.tg_table_schema_slot] = Value::Text(call.table_schema.clone().into());
}

pub fn seed_event_trigger_state_values(
    bindings: &CompiledEventTriggerBindings,
    call: &EventTriggerCallContext,
    values: &mut [Value],
) {
    values[bindings.tg_event_slot] = Value::Text(call.event.clone().into());
    values[bindings.tg_tag_slot] = Value::Text(call.tag.clone().into());
}

pub fn seed_trigger_relation_values(
    relation: &CompiledTriggerRelation,
    source: Option<&Vec<Value>>,
    values: &mut [Value],
) {
    let Some(source) = source else {
        return;
    };
    for (slot, value) in relation.slots.iter().copied().zip(source.iter()) {
        values[slot] = value.clone();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DynamicQuerySubstitutionError<E> {
    InvalidParameterReference(String),
    MissingParameter(usize),
    MissingCursorParameter(String),
    Render(E),
}

pub fn substitute_dynamic_query_params<E>(
    sql: &str,
    params: &[Value],
    mut render_param: impl FnMut(usize, &Value) -> Result<String, E>,
) -> Result<String, DynamicQuerySubstitutionError<E>> {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut idx = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        if copy_quoted_sql(
            sql,
            bytes,
            &mut out,
            &mut idx,
            &mut in_single,
            &mut in_double,
        ) {
            continue;
        }

        match ch {
            '\'' => {
                in_single = true;
                out.push(ch);
                idx += 1;
                continue;
            }
            '"' => {
                in_double = true;
                out.push(ch);
                idx += 1;
                continue;
            }
            '$' => {
                let mut end = idx + 1;
                while let Some(byte) = bytes.get(end) {
                    if !byte.is_ascii_digit() {
                        break;
                    }
                    end += 1;
                }
                if end > idx + 1 && (end == bytes.len() || !is_identifier_char(bytes[end] as char))
                {
                    let index = sql[idx + 1..end].parse::<usize>().map_err(|_| {
                        DynamicQuerySubstitutionError::InvalidParameterReference(
                            sql[idx..end].to_string(),
                        )
                    })?;
                    let value = params
                        .get(index.saturating_sub(1))
                        .ok_or(DynamicQuerySubstitutionError::MissingParameter(index))?;
                    out.push_str(
                        &render_param(index, value)
                            .map_err(DynamicQuerySubstitutionError::Render)?,
                    );
                    idx = end;
                    continue;
                }
            }
            _ => {}
        }

        out.push(ch);
        idx += 1;
    }
    Ok(out)
}

pub fn substitute_declared_cursor_params<E>(
    sql: &str,
    params: &[DeclaredCursorParam],
    values: &[Value],
    mut render_param: impl FnMut(&Value, &DeclaredCursorParam) -> Result<String, E>,
) -> Result<String, DynamicQuerySubstitutionError<E>> {
    if params.is_empty() {
        return Ok(sql.to_string());
    }
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut idx = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        if copy_quoted_sql(
            sql,
            bytes,
            &mut out,
            &mut idx,
            &mut in_single,
            &mut in_double,
        ) {
            continue;
        }

        match ch {
            '\'' => {
                in_single = true;
                out.push(ch);
                idx += 1;
                continue;
            }
            '"' => {
                in_double = true;
                out.push(ch);
                idx += 1;
                continue;
            }
            _ if is_identifier_start(ch) => {
                let start = idx;
                idx += 1;
                while idx < bytes.len() && is_identifier_char(bytes[idx] as char) {
                    idx += 1;
                }
                let ident = &sql[start..idx];
                if let Some(param_index) = params
                    .iter()
                    .position(|param| param.name.eq_ignore_ascii_case(ident))
                {
                    let value = values.get(param_index).ok_or_else(|| {
                        DynamicQuerySubstitutionError::MissingCursorParameter(ident.to_string())
                    })?;
                    out.push_str(
                        &render_param(value, &params[param_index])
                            .map_err(DynamicQuerySubstitutionError::Render)?,
                    );
                } else {
                    out.push_str(ident);
                }
                continue;
            }
            _ => {}
        }

        out.push(ch);
        idx += 1;
    }
    Ok(out)
}

pub fn dynamic_drop_table_undroppable_notice(identity: &str) -> Option<String> {
    let (schema, table) = identity.split_once('.')?;
    (schema == "audit_tbls").then(|| format!("table \"{schema}_{table}\" does not exist, skipping"))
}

pub fn event_trigger_drop_table_command_rows_for_dynamic_sql(
    stmt: &DropTableStatement,
    catalog: &dyn CatalogLookup,
) -> Vec<EventTriggerDdlCommandRow> {
    stmt.table_names
        .iter()
        .filter_map(|name| catalog.lookup_any_relation(name))
        .map(|relation| {
            let (schema, table, _) = event_trigger_relation_schema_and_name(catalog, &relation);
            EventTriggerDdlCommandRow {
                command_tag: "DROP TABLE".into(),
                object_type: "table".into(),
                schema_name: Some(schema.clone()),
                object_identity: qualified_event_identity(&schema, &table),
            }
        })
        .collect()
}

pub fn event_trigger_dropped_table_rows_for_dynamic_sql(
    stmt: &DropTableStatement,
    catalog: &dyn CatalogLookup,
) -> Vec<EventTriggerDroppedObjectRow> {
    stmt.table_names
        .iter()
        .filter_map(|name| catalog.lookup_any_relation(name))
        .flat_map(|relation| event_trigger_dropped_table_rows(catalog, &relation))
        .collect()
}

pub fn event_trigger_dropped_table_rows(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
) -> Vec<EventTriggerDroppedObjectRow> {
    // :HACK: PL/pgSQL dynamic DDL goes around Session's event-trigger row
    // collector. This mirrors the table/type rows needed by event_trigger.sql;
    // dependency-driven object collection should eventually live in the drop
    // executor and be shared by both paths.
    let (schema, table, is_temporary) = event_trigger_relation_schema_and_name(catalog, relation);
    let qualified_table = qualified_event_identity(&schema, &table);
    vec![
        event_trigger_dropped_object_row(
            "table",
            Some(schema.clone()),
            Some(table.clone()),
            qualified_table.clone(),
            vec![schema.clone(), table.clone()],
            true,
            false,
            is_temporary,
        ),
        event_trigger_dropped_object_row(
            "type",
            Some(schema.clone()),
            Some(table.clone()),
            qualified_table.clone(),
            vec![qualified_table.clone()],
            false,
            false,
            is_temporary,
        ),
        event_trigger_dropped_object_row(
            "type",
            Some(schema.clone()),
            Some(format!("_{table}")),
            format!("{qualified_table}[]"),
            vec![format!("{qualified_table}[]")],
            false,
            false,
            is_temporary,
        ),
    ]
}

pub fn event_trigger_relation_schema_and_name(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
) -> (String, String, bool) {
    let is_temporary = relation.relpersistence == 't';
    let schema = if is_temporary {
        "pg_temp".into()
    } else {
        catalog
            .namespace_row_by_oid(relation.namespace_oid)
            .map(|row| row.nspname)
            .unwrap_or_else(|| "public".into())
    };
    let table = catalog
        .class_row_by_oid(relation.relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation.relation_oid.to_string());
    (schema, table, is_temporary)
}

pub fn event_trigger_dropped_object_row(
    object_type: &str,
    schema_name: Option<String>,
    object_name: Option<String>,
    object_identity: String,
    address_names: Vec<String>,
    original: bool,
    normal: bool,
    is_temporary: bool,
) -> EventTriggerDroppedObjectRow {
    EventTriggerDroppedObjectRow {
        classid: 0,
        objid: 0,
        objsubid: 0,
        original,
        normal,
        is_temporary,
        object_type: object_type.into(),
        schema_name,
        object_name,
        object_identity,
        address_names,
        address_args: Vec::new(),
    }
}

pub fn qualified_event_identity(schema: &str, object_name: &str) -> String {
    format!(
        "{}.{}",
        quote_identifier_if_needed(schema),
        quote_identifier_if_needed(object_name)
    )
}

pub fn quote_identifier_if_needed(identifier: &str) -> String {
    if !identifier.is_empty()
        && identifier.chars().enumerate().all(|(idx, ch)| {
            if idx == 0 {
                ch == '_' || ch.is_ascii_lowercase()
            } else {
                ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit()
            }
        })
    {
        return identifier.into();
    }
    let escaped = identifier.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn copy_quoted_sql(
    sql: &str,
    bytes: &[u8],
    out: &mut String,
    idx: &mut usize,
    in_single: &mut bool,
    in_double: &mut bool,
) -> bool {
    if *in_single {
        let ch = bytes[*idx] as char;
        out.push(ch);
        *idx += 1;
        if ch == '\'' {
            if bytes.get(*idx) == Some(&b'\'') {
                out.push('\'');
                *idx += 1;
                return true;
            }
            *in_single = false;
        }
        return true;
    }
    if *in_double {
        let ch = bytes[*idx] as char;
        out.push(ch);
        *idx += 1;
        if ch == '"' {
            if bytes.get(*idx) == Some(&b'"') {
                out.push('"');
                *idx += 1;
                return true;
            }
            *in_double = false;
        }
        return true;
    }
    if let Some(tag) = dollar_quote_tag_at(sql, *idx) {
        if let Some(close) = sql[*idx + tag.len()..].find(tag) {
            let end = *idx + tag.len() + close + tag.len();
            out.push_str(&sql[*idx..end]);
            *idx = end;
        } else {
            out.push_str(&sql[*idx..]);
            *idx = bytes.len();
        }
        return true;
    }
    false
}

pub fn cursor_direction_is_forward_only(direction: CursorDirection) -> bool {
    matches!(
        direction,
        CursorDirection::Next | CursorDirection::Forward(_)
    )
}

pub fn cursor_fetch(
    cursor: &mut FunctionCursor,
    direction: CursorDirection,
) -> Option<FunctionQueryRow> {
    let target = cursor_target_position(cursor, direction)?;
    if target >= 0 && (target as usize) < cursor.rows.len() {
        cursor.current = target;
        return cursor.rows.get(target as usize).cloned();
    }
    cursor.current = target.clamp(-1, cursor.rows.len() as isize);
    None
}

pub fn cursor_move(cursor: &mut FunctionCursor, direction: CursorDirection) -> bool {
    match direction {
        CursorDirection::ForwardAll => {
            let old = cursor.current;
            cursor.current = cursor.rows.len() as isize;
            cursor.current != old
        }
        CursorDirection::BackwardAll => {
            let old = cursor.current;
            cursor.current = -1;
            cursor.current != old
        }
        _ => {
            let Some(target) = cursor_target_position(cursor, direction) else {
                return false;
            };
            let clamped = target.clamp(-1, cursor.rows.len() as isize);
            let moved = clamped != cursor.current;
            cursor.current = clamped;
            moved && target >= 0 && (target as usize) < cursor.rows.len()
        }
    }
}

pub fn cursor_target_position(
    cursor: &FunctionCursor,
    direction: CursorDirection,
) -> Option<isize> {
    let len = cursor.rows.len() as isize;
    Some(match direction {
        CursorDirection::Next => cursor.current + 1,
        CursorDirection::Prior => cursor.current - 1,
        CursorDirection::First => 0,
        CursorDirection::Last => len.checked_sub(1)?,
        CursorDirection::Forward(count) => cursor.current + count as isize,
        CursorDirection::Backward(count) => cursor.current - count as isize,
        CursorDirection::ForwardAll => len,
        CursorDirection::BackwardAll => -1,
        CursorDirection::Absolute(index) => {
            if index > 0 {
                index as isize - 1
            } else if index < 0 {
                len + index as isize
            } else {
                -1
            }
        }
        CursorDirection::Relative(count) => cursor.current + count as isize,
    })
}

pub fn current_cursor_system_binding(cursor: &FunctionCursor) -> Option<SystemVarBinding> {
    if cursor.current < 0 || cursor.current as usize >= cursor.rows.len() {
        return None;
    }
    cursor.rows[cursor.current as usize]
        .system_bindings
        .iter()
        .find(|binding| binding.tid.is_some())
        .copied()
}

pub fn current_of_predicate(binding: SystemVarBinding) -> Option<Expr> {
    let tid = binding.tid?;
    let tableoid = Expr::Var(Var {
        varno: 1,
        varattno: TABLE_OID_ATTR_NO,
        varlevelsup: 0,
        vartype: SqlType::new(SqlTypeKind::Oid),
        collation_oid: None,
    });
    let ctid = Expr::Var(Var {
        varno: 1,
        varattno: SELF_ITEM_POINTER_ATTR_NO,
        varlevelsup: 0,
        vartype: SqlType::new(SqlTypeKind::Tid),
        collation_oid: None,
    });
    Some(Expr::and(
        Expr::op_auto(
            OpExprKind::Eq,
            vec![
                tableoid,
                Expr::Const(Value::Int64(i64::from(binding.table_oid))),
            ],
        ),
        Expr::op_auto(OpExprKind::Eq, vec![ctid, Expr::Const(Value::Tid(tid))]),
    ))
}

#[derive(Debug, Clone)]
pub struct DynamicExternalParamBinding {
    pub paramid: usize,
    pub expr: Expr,
    pub ty: SqlType,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CompiledTriggerBindings, CompiledTriggerRelation, FunctionReturnContract};
    use pgrust_nodes::parsenodes::{TriggerLevel, TriggerTiming};
    use pgrust_nodes::primnodes::RelationDesc;
    use pgrust_nodes::{TriggerFunctionResult, TriggerOperation};

    fn cursor(len: usize, current: isize) -> FunctionCursor {
        FunctionCursor {
            columns: Vec::new(),
            rows: (0..len)
                .map(|value| FunctionQueryRow {
                    values: vec![Value::Int64(value as i64)],
                    system_bindings: Vec::new(),
                })
                .collect(),
            current,
            scrollable: true,
        }
    }

    #[test]
    fn cursor_fetch_updates_position_and_returns_row() {
        let mut cursor = cursor(3, -1);

        let row = cursor_fetch(&mut cursor, CursorDirection::Next).unwrap();

        assert_eq!(cursor.current, 0);
        assert_eq!(row.values, vec![Value::Int64(0)]);
    }

    #[test]
    fn cursor_move_clamps_out_of_range_targets() {
        let mut cursor = cursor(2, 0);

        assert!(!cursor_move(&mut cursor, CursorDirection::Forward(5)));
        assert_eq!(cursor.current, 2);
        assert!(cursor_move(&mut cursor, CursorDirection::BackwardAll));
        assert_eq!(cursor.current, -1);
    }

    #[test]
    fn trigger_outer_tuple_appends_new_and_old_records() {
        let contract = trigger_contract();
        let values = vec![
            Value::Int32(11),
            Value::Text("new".into()),
            Value::Int32(22),
            Value::Text("old".into()),
        ];

        let outer = function_outer_tuple(&contract, &values);

        assert_eq!(outer.len(), 6);
        assert_eq!(outer[..4], values[..]);
        assert!(matches!(&outer[4], Value::Record(record) if record.fields == values[..2]));
        assert!(matches!(&outer[5], Value::Record(record) if record.fields == values[2..]));
    }

    #[test]
    fn current_trigger_return_selects_requested_row() {
        let contract = trigger_contract();
        let values = vec![
            Value::Int32(11),
            Value::Text("new".into()),
            Value::Int32(22),
            Value::Text("old".into()),
        ];

        let returned = current_trigger_return(&contract, &values, TriggerReturnedRow::Old);

        assert!(matches!(
            returned,
            Some(TriggerFunctionResult::ReturnOld(row))
                if row == vec![Value::Int32(22), Value::Text("old".into())]
        ));
    }

    #[test]
    fn dynamic_query_substitution_skips_quoted_sql() {
        let params = vec![Value::Int32(7)];

        let sql = substitute_dynamic_query_params(
            "select $1, '$1', \"col$1\", $$ $1 $$",
            &params,
            |_, value| Ok::<_, ()>(format!("{value:?}")),
        )
        .unwrap();

        assert_eq!(sql, "select Int32(7), '$1', \"col$1\", $$ $1 $$");
    }

    #[test]
    fn declared_cursor_substitution_replaces_named_params() {
        let params = vec![DeclaredCursorParam {
            name: "p_id".into(),
            type_name: "integer".into(),
            ty: SqlType::new(SqlTypeKind::Int4),
        }];
        let values = vec![Value::Int32(42)];

        let sql = substitute_declared_cursor_params(
            "select p_id, p_id2, 'p_id'",
            &params,
            &values,
            |value, _| Ok::<_, ()>(format!("{value:?}")),
        )
        .unwrap();

        assert_eq!(sql, "select Int32(42), p_id2, 'p_id'");
    }

    #[test]
    fn event_trigger_identity_quotes_only_when_needed() {
        assert_eq!(
            qualified_event_identity("public", "plain_name"),
            "public.plain_name"
        );
        assert_eq!(
            qualified_event_identity("audit tbls", "MixedName"),
            "\"audit tbls\".\"MixedName\""
        );
    }

    #[test]
    fn dynamic_drop_notice_is_limited_to_audit_tables() {
        assert_eq!(
            dynamic_drop_table_undroppable_notice("audit_tbls.events"),
            Some("table \"audit_tbls_events\" does not exist, skipping".into())
        );
        assert_eq!(dynamic_drop_table_undroppable_notice("public.events"), None);
    }

    #[test]
    fn seed_trigger_state_values_uses_zero_based_tg_argv() {
        let bindings = trigger_bindings();
        let call = TriggerCallContext {
            relation_desc: RelationDesc { columns: vec![] },
            relation_oid: 42,
            table_name: "main_table".into(),
            table_schema: "public".into(),
            trigger_name: "before_ins_stmt".into(),
            trigger_args: vec!["arg0".into()],
            timing: TriggerTiming::Before,
            level: TriggerLevel::Statement,
            op: TriggerOperation::Insert,
            new_row: None,
            old_row: None,
            transition_tables: Vec::new(),
        };
        let mut values = vec![Value::Null; 9];

        seed_trigger_state_values(&bindings, &call, &mut values);

        assert_eq!(values[bindings.tg_nargs_slot], Value::Int32(1));
        assert_eq!(
            values[bindings.tg_argv_slot],
            Value::PgArray(
                ArrayValue::from_dimensions(
                    vec![ArrayDimension {
                        lower_bound: 0,
                        length: 1,
                    }],
                    vec![Value::Text("arg0".into())],
                )
                .with_element_type_oid(TEXT_TYPE_OID),
            )
        );
    }

    fn trigger_contract() -> FunctionReturnContract {
        FunctionReturnContract::Trigger {
            bindings: trigger_bindings(),
        }
    }

    fn trigger_bindings() -> CompiledTriggerBindings {
        CompiledTriggerBindings {
            new_row: CompiledTriggerRelation {
                slots: vec![0, 1],
                field_names: vec!["id".into(), "name".into()],
                field_types: vec![
                    SqlType::new(SqlTypeKind::Int4),
                    SqlType::new(SqlTypeKind::Text),
                ],
                not_null: vec![true, false],
            },
            old_row: CompiledTriggerRelation {
                slots: vec![2, 3],
                field_names: vec!["id".into(), "name".into()],
                field_types: vec![
                    SqlType::new(SqlTypeKind::Int4),
                    SqlType::new(SqlTypeKind::Text),
                ],
                not_null: vec![true, false],
            },
            tg_name_slot: 4,
            tg_op_slot: 5,
            tg_when_slot: 6,
            tg_level_slot: 7,
            tg_relid_slot: 8,
            tg_nargs_slot: 5,
            tg_argv_slot: 6,
            tg_table_name_slot: 7,
            tg_table_schema_slot: 8,
        }
    }
}
