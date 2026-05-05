use pgrust_analyze::{
    BoundCte, BoundDeleteStatement, BoundInsertStatement, BoundUpdateStatement, SlotScopeColumn,
};
use pgrust_nodes::SqlType;
use pgrust_nodes::parsenodes::{
    CommentOnFunctionStatement, CreateTableAsStatement, CreateTableStatement,
};
use pgrust_nodes::plannodes::{Plan, PlannedStmt};
use pgrust_nodes::primnodes::{Expr, QueryColumn};

use crate::{CursorDirection, ExceptionCondition, QueryCompareOp, RaiseLevel};

#[derive(Debug, Clone)]
pub struct CompiledBlock {
    pub local_slots: Vec<CompiledVar>,
    pub statements: Vec<CompiledStmt>,
    pub exception_handlers: Vec<CompiledExceptionHandler>,
    pub exception_sqlstate_slot: Option<usize>,
    pub exception_sqlerrm_slot: Option<usize>,
    pub total_slots: usize,
}

#[derive(Debug, Clone)]
pub struct CompiledFunction {
    pub name: String,
    pub proc_oid: u32,
    pub proowner: u32,
    pub prosecdef: bool,
    pub provolatile: char,
    pub proconfig: Option<Vec<String>>,
    pub print_strict_params: Option<bool>,
    pub parameter_slots: Vec<CompiledFunctionSlot>,
    pub context_arg_type_names: Vec<String>,
    pub output_slots: Vec<CompiledOutputSlot>,
    pub body: CompiledBlock,
    pub return_contract: FunctionReturnContract,
    pub found_slot: usize,
    pub sqlstate_slot: usize,
    pub sqlerrm_slot: usize,
    pub local_ctes: Vec<BoundCte>,
    pub trigger_transition_ctes: Vec<CompiledTriggerTransitionCte>,
}

pub fn collect_compiled_slot_names(compiled: &CompiledFunction, out: &mut Vec<(String, usize)>) {
    out.extend(
        compiled
            .parameter_slots
            .iter()
            .map(|slot| (slot.name.clone(), slot.slot)),
    );
    out.extend(
        compiled
            .output_slots
            .iter()
            .map(|slot| (slot.name.clone(), slot.slot)),
    );
    collect_block_slot_names(&compiled.body, out);
}

pub fn collect_block_slot_names(block: &CompiledBlock, out: &mut Vec<(String, usize)>) {
    out.extend(
        block
            .local_slots
            .iter()
            .map(|slot| (slot.name.clone(), slot.slot)),
    );
    for stmt in &block.statements {
        collect_stmt_slot_names(stmt, out);
    }
    for handler in &block.exception_handlers {
        for stmt in &handler.statements {
            collect_stmt_slot_names(stmt, out);
        }
    }
}

pub fn collect_stmt_slot_names(stmt: &CompiledStmt, out: &mut Vec<(String, usize)>) {
    match stmt {
        CompiledStmt::WithLine { stmt, .. } => collect_stmt_slot_names(stmt, out),
        CompiledStmt::Block(block) => collect_block_slot_names(block, out),
        CompiledStmt::If {
            branches,
            else_branch,
        } => {
            for (_, body) in branches {
                for stmt in body {
                    collect_stmt_slot_names(stmt, out);
                }
            }
            for stmt in else_branch {
                collect_stmt_slot_names(stmt, out);
            }
        }
        CompiledStmt::While { body, .. }
        | CompiledStmt::Loop { body }
        | CompiledStmt::ForInt { body, .. }
        | CompiledStmt::ForQuery { body, .. }
        | CompiledStmt::ForEach { body, .. } => {
            for stmt in body {
                collect_stmt_slot_names(stmt, out);
            }
        }
        _ => {}
    }
}

#[derive(Debug, Clone)]
pub struct CompiledFunctionSlot {
    pub name: String,
    pub slot: usize,
    pub ty: SqlType,
}

#[derive(Debug, Clone)]
pub struct CompiledTriggerTransitionCte {
    pub name: String,
    pub cte_id: usize,
}

#[derive(Debug, Clone)]
pub struct CompiledOutputSlot {
    pub name: String,
    pub slot: usize,
    pub column: QueryColumn,
}

#[derive(Debug, Clone)]
pub struct CompiledVar {
    pub name: String,
    pub slot: usize,
    pub ty: SqlType,
    pub default_expr: Option<CompiledExpr>,
    pub not_null: bool,
    pub line: usize,
}

#[derive(Debug, Clone)]
pub enum CompiledExpr {
    Scalar {
        expr: Expr,
        subplans: Vec<Plan>,
        source: String,
    },
    QueryCompare {
        plan: PlannedStmt,
        op: QueryCompareOp,
        rhs: Expr,
        source: String,
    },
    DeferredError {
        source: String,
        err: pgrust_nodes::parsenodes::ParseError,
    },
}

#[derive(Debug, Clone)]
pub struct CompiledSelectIntoTarget {
    pub slot: usize,
    pub ty: SqlType,
    pub name: Option<String>,
    pub not_null: bool,
}

#[derive(Debug, Clone)]
pub struct CompiledIndirectAssignTarget {
    pub slot: usize,
    pub ty: SqlType,
    pub indirection: Vec<CompiledAssignIndirection>,
}

#[derive(Debug, Clone)]
pub enum CompiledAssignIndirection {
    Field(String),
    Subscript(CompiledExpr),
}

#[derive(Debug, Clone)]
pub struct CompiledStrictParam {
    pub name: String,
    pub slot: usize,
}

#[derive(Debug, Clone)]
pub struct DeclaredCursorParam {
    pub name: String,
    pub type_name: String,
    pub ty: SqlType,
}

#[derive(Debug, Clone)]
pub struct CompiledForQueryTarget {
    pub targets: Vec<CompiledSelectIntoTarget>,
}

#[derive(Debug, Clone)]
pub struct RuntimeSqlScope {
    pub columns: Vec<SlotScopeColumn>,
    pub relation_scopes: Vec<(String, Vec<SlotScopeColumn>)>,
}

#[derive(Debug, Clone)]
pub enum CompiledForQuerySource {
    Static {
        plan: PlannedStmt,
    },
    Runtime {
        sql: String,
        scope: RuntimeSqlScope,
    },
    NoTuples {
        sql: String,
    },
    Dynamic {
        sql_expr: CompiledExpr,
        using_exprs: Vec<CompiledExpr>,
    },
    Cursor {
        slot: usize,
        name: String,
        source: CompiledCursorOpenSource,
        scrollable: bool,
    },
}

#[derive(Debug, Clone)]
pub enum CompiledCursorOpenSource {
    Static {
        plan: PlannedStmt,
    },
    Dynamic {
        sql_expr: CompiledExpr,
        using_exprs: Vec<CompiledExpr>,
    },
    Declared {
        query: String,
        params: Vec<DeclaredCursorParam>,
        args: Vec<CompiledExpr>,
        arg_context: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct CompiledExceptionHandler {
    pub conditions: Vec<ExceptionCondition>,
    pub statements: Vec<CompiledStmt>,
}

#[derive(Debug, Clone)]
pub enum FunctionReturnContract {
    Scalar {
        ty: SqlType,
        setof: bool,
        output_slot: Option<usize>,
    },
    FixedRow {
        columns: Vec<QueryColumn>,
        setof: bool,
        uses_output_vars: bool,
        composite_typrelid: Option<u32>,
    },
    AnonymousRecord {
        setof: bool,
    },
    Trigger {
        bindings: CompiledTriggerBindings,
    },
    EventTrigger {
        bindings: CompiledEventTriggerBindings,
    },
}

#[derive(Debug, Clone)]
pub struct CompiledTriggerBindings {
    pub new_row: CompiledTriggerRelation,
    pub old_row: CompiledTriggerRelation,
    pub tg_name_slot: usize,
    pub tg_op_slot: usize,
    pub tg_when_slot: usize,
    pub tg_level_slot: usize,
    pub tg_relid_slot: usize,
    pub tg_nargs_slot: usize,
    pub tg_argv_slot: usize,
    pub tg_table_name_slot: usize,
    pub tg_table_schema_slot: usize,
}

#[derive(Debug, Clone)]
pub struct CompiledTriggerRelation {
    pub slots: Vec<usize>,
    pub field_names: Vec<String>,
    pub field_types: Vec<SqlType>,
    pub not_null: Vec<bool>,
}

#[derive(Debug, Clone)]
pub struct CompiledEventTriggerBindings {
    pub tg_event_slot: usize,
    pub tg_tag_slot: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerReturnedRow {
    New,
    Old,
}

#[derive(Debug, Clone)]
pub enum CompiledStmt {
    WithLine {
        line: usize,
        stmt: Box<CompiledStmt>,
    },
    Block(CompiledBlock),
    Assign {
        slot: usize,
        ty: SqlType,
        name: Option<String>,
        not_null: bool,
        expr: CompiledExpr,
        line: usize,
    },
    AssignSubscript {
        slot: usize,
        root_ty: SqlType,
        target_ty: SqlType,
        subscripts: Vec<CompiledExpr>,
        expr: CompiledExpr,
        line: usize,
    },
    AssignIndirect {
        target: CompiledIndirectAssignTarget,
        expr: CompiledExpr,
        line: usize,
    },
    AssignTriggerRow {
        row: TriggerReturnedRow,
        expr: CompiledExpr,
        line: usize,
    },
    Null,
    If {
        branches: Vec<(CompiledExpr, Vec<CompiledStmt>)>,
        else_branch: Vec<CompiledStmt>,
    },
    While {
        condition: CompiledExpr,
        body: Vec<CompiledStmt>,
    },
    Loop {
        body: Vec<CompiledStmt>,
    },
    Exit {
        condition: Option<CompiledExpr>,
    },
    ForInt {
        slot: usize,
        start_expr: CompiledExpr,
        end_expr: CompiledExpr,
        body: Vec<CompiledStmt>,
    },
    ForQuery {
        target: CompiledForQueryTarget,
        source: CompiledForQuerySource,
        body: Vec<CompiledStmt>,
    },
    ForEach {
        target: CompiledForQueryTarget,
        slice: usize,
        array_expr: CompiledExpr,
        body: Vec<CompiledStmt>,
    },
    Raise {
        level: RaiseLevel,
        sqlstate: Option<String>,
        message: Option<String>,
        message_expr: Option<CompiledExpr>,
        detail_expr: Option<CompiledExpr>,
        hint_expr: Option<CompiledExpr>,
        errcode_expr: Option<CompiledExpr>,
        column_expr: Option<CompiledExpr>,
        constraint_expr: Option<CompiledExpr>,
        datatype_expr: Option<CompiledExpr>,
        table_expr: Option<CompiledExpr>,
        schema_expr: Option<CompiledExpr>,
        params: Vec<CompiledExpr>,
        option_error: Option<pgrust_nodes::parsenodes::ParseError>,
        line: usize,
    },
    Reraise,
    Assert {
        condition: CompiledExpr,
        message: Option<CompiledExpr>,
    },
    Continue {
        condition: Option<CompiledExpr>,
    },
    Return {
        expr: Option<CompiledExpr>,
        line: usize,
    },
    ReturnRuntimeQuery {
        sql: String,
        scope: RuntimeSqlScope,
        line: usize,
    },
    ReturnSelect {
        plan: PlannedStmt,
        sql: String,
        line: usize,
    },
    ReturnNext {
        expr: Option<CompiledExpr>,
    },
    ReturnTriggerRow {
        row: TriggerReturnedRow,
    },
    ReturnTriggerNull,
    ReturnTriggerNoValue,
    ReturnQuery {
        source: CompiledForQuerySource,
    },
    Perform {
        plan: PlannedStmt,
        line: usize,
        sql: Option<String>,
    },
    DynamicExecute {
        sql_expr: CompiledExpr,
        strict: bool,
        into_targets: Vec<CompiledSelectIntoTarget>,
        using_exprs: Vec<CompiledExpr>,
        line: usize,
    },
    SetGuc {
        name: String,
        value: Option<String>,
        is_local: bool,
    },
    CommentOnFunction {
        stmt: CommentOnFunctionStatement,
    },
    GetDiagnostics {
        stacked: bool,
        items: Vec<(CompiledSelectIntoTarget, String)>,
    },
    OpenCursor {
        slot: usize,
        name: String,
        source: CompiledCursorOpenSource,
        scrollable: bool,
        constant: bool,
    },
    FetchCursor {
        slot: usize,
        direction: CursorDirection,
        targets: Vec<CompiledSelectIntoTarget>,
    },
    MoveCursor {
        slot: usize,
        direction: CursorDirection,
    },
    CloseCursor {
        slot: usize,
    },
    UnsupportedTransactionCommand {
        command: String,
    },
    SelectInto {
        plan: PlannedStmt,
        targets: Vec<CompiledSelectIntoTarget>,
        strict: bool,
        strict_params: Vec<CompiledStrictParam>,
    },
    ExecInsertInto {
        stmt: BoundInsertStatement,
        targets: Vec<CompiledSelectIntoTarget>,
    },
    ExecInsert {
        stmt: BoundInsertStatement,
    },
    ExecUpdateInto {
        stmt: BoundUpdateStatement,
        scope: RuntimeSqlScope,
        targets: Vec<CompiledSelectIntoTarget>,
    },
    ExecUpdate {
        stmt: BoundUpdateStatement,
        scope: RuntimeSqlScope,
    },
    ExecDeleteInto {
        stmt: BoundDeleteStatement,
        targets: Vec<CompiledSelectIntoTarget>,
    },
    ExecDelete {
        stmt: BoundDeleteStatement,
    },
    RuntimeSql {
        sql: String,
        scope: RuntimeSqlScope,
    },
    RuntimeSelectInto {
        sql: String,
        scope: RuntimeSqlScope,
        targets: Vec<CompiledSelectIntoTarget>,
        strict: bool,
        strict_params: Vec<CompiledStrictParam>,
    },
    CreateTableAs {
        stmt: CreateTableAsStatement,
    },
    CreateTable {
        stmt: CreateTableStatement,
    },
    ExecSql {
        sql: String,
    },
}
