#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::Expr;
use crate::backend::executor::RelationDesc;
use crate::backend::optimizer::finalize_expr_subqueries;
use crate::backend::parser::analyze::scope_for_relation;
use crate::backend::parser::{
    AliasColumnSpec, ArraySubscript, Assignment, AssignmentTarget, AssignmentTargetIndirection,
    BoundCte, BoundDeleteStatement, BoundInsertStatement, BoundScope, BoundUpdateStatement,
    CatalogLookup, CommentOnFunctionStatement, CreateTableAsStatement, CreateTableStatement,
    CteBody, DeleteStatement, FromItem, GroupByItem, InsertSource, InsertStatement, MergeAction,
    MergeInsertSource, MergeStatement, OnConflictAction, OnConflictClause, OnConflictTarget,
    OrderByItem, ParseError, RawWindowFrame, RawWindowFrameBound, RawWindowSpec, RawXmlExpr,
    SelectItem, SelectStatement, SlotScopeColumn, SqlCallArgs, SqlCaseWhen, SqlExpr, SqlType,
    SqlTypeKind, Statement, TablePersistence, UpdateStatement, ValuesStatement, XmlTableColumn,
    bind_delete_with_outer_scopes, bind_insert_with_outer_scopes,
    bind_scalar_expr_in_named_slot_scope, bind_update_with_outer_scopes, parse_expr,
    parse_statement, parse_type_name, pg_plan_query_with_outer_scopes_and_ctes,
    pg_plan_query_with_outer_scopes_and_ctes_config,
    pg_plan_values_query_with_outer_scopes_and_ctes,
    pg_plan_values_query_with_outer_scopes_and_ctes_config, resolve_raw_type_name,
};
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::include::catalog::{EVENT_TRIGGER_TYPE_OID, PgProcRow, RECORD_TYPE_OID};
use crate::include::nodes::pathnodes::PlannerConfig;
use crate::include::nodes::plannodes::{Plan, PlannedStmt};
use crate::include::nodes::primnodes::{
    Param, ParamKind, QueryColumn, TargetEntry, Var, user_attrno,
};

use super::ast::{
    AliasTarget, AssignTarget, Block, CursorArg, CursorDecl, CursorDirection, Decl,
    ExceptionCondition, ForQuerySource, ForTarget, OpenCursorSource, RaiseCondition, RaiseLevel,
    RaiseUsingOption, Stmt, VarDecl,
};
use super::gram::parse_block;

#[derive(Debug, Clone)]
pub(crate) struct CompiledBlock {
    pub(crate) local_slots: Vec<CompiledVar>,
    pub(crate) statements: Vec<CompiledStmt>,
    pub(crate) exception_handlers: Vec<CompiledExceptionHandler>,
    pub(crate) exception_sqlstate_slot: Option<usize>,
    pub(crate) exception_sqlerrm_slot: Option<usize>,
    pub(crate) total_slots: usize,
}

#[derive(Debug, Clone)]
pub struct CompiledFunction {
    pub(crate) name: String,
    pub(crate) proc_oid: u32,
    pub(crate) proowner: u32,
    pub(crate) prosecdef: bool,
    pub(crate) provolatile: char,
    pub(crate) proconfig: Option<Vec<String>>,
    pub(crate) print_strict_params: Option<bool>,
    pub(crate) parameter_slots: Vec<CompiledFunctionSlot>,
    pub(crate) context_arg_type_names: Vec<String>,
    pub(crate) output_slots: Vec<CompiledOutputSlot>,
    pub(crate) body: CompiledBlock,
    pub(crate) return_contract: FunctionReturnContract,
    pub(crate) found_slot: usize,
    pub(crate) sqlstate_slot: usize,
    pub(crate) sqlerrm_slot: usize,
    pub(crate) local_ctes: Vec<BoundCte>,
    pub(crate) trigger_transition_ctes: Vec<CompiledTriggerTransitionCte>,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledFunctionSlot {
    pub(crate) name: String,
    pub(crate) slot: usize,
    pub(crate) ty: SqlType,
}

#[derive(Debug, Clone)]
pub struct TriggerTransitionTable {
    pub name: String,
    pub desc: RelationDesc,
    pub rows: Vec<Vec<crate::backend::executor::Value>>,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledTriggerTransitionCte {
    pub(crate) name: String,
    pub(crate) cte_id: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledOutputSlot {
    pub(crate) name: String,
    pub(crate) slot: usize,
    pub(crate) column: QueryColumn,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledVar {
    pub(crate) name: String,
    pub(crate) slot: usize,
    pub(crate) ty: SqlType,
    pub(crate) default_expr: Option<CompiledExpr>,
    pub(crate) not_null: bool,
    pub(crate) line: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum PlpgsqlVariableConflict {
    #[default]
    Error,
    UseVariable,
    UseColumn,
}

#[derive(Debug, Clone)]
pub(crate) enum CompiledExpr {
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryCompareOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledSelectIntoTarget {
    pub(crate) slot: usize,
    pub(crate) ty: SqlType,
    pub(crate) name: Option<String>,
    pub(crate) not_null: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledIndirectAssignTarget {
    pub(crate) slot: usize,
    pub(crate) ty: SqlType,
    pub(crate) indirection: Vec<CompiledAssignIndirection>,
}

#[derive(Debug, Clone)]
pub(crate) enum CompiledAssignIndirection {
    Field(String),
    Subscript(CompiledExpr),
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledStrictParam {
    pub(crate) name: String,
    pub(crate) slot: usize,
}

#[derive(Debug, Clone)]
struct DeclaredCursor {
    query: String,
    scrollable: bool,
    params: Vec<DeclaredCursorParam>,
}

#[derive(Debug, Clone)]
pub(crate) struct DeclaredCursorParam {
    pub(crate) name: String,
    pub(crate) type_name: String,
    pub(crate) ty: SqlType,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledForQueryTarget {
    pub(crate) targets: Vec<CompiledSelectIntoTarget>,
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeSqlScope {
    pub(crate) columns: Vec<SlotScopeColumn>,
    pub(crate) relation_scopes: Vec<(String, Vec<SlotScopeColumn>)>,
}

#[derive(Debug, Clone)]
pub(crate) enum CompiledForQuerySource {
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
pub(crate) enum CompiledCursorOpenSource {
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
pub(crate) struct CompiledExceptionHandler {
    pub(crate) conditions: Vec<ExceptionCondition>,
    pub(crate) statements: Vec<CompiledStmt>,
}

#[derive(Debug, Clone)]
pub(crate) enum FunctionReturnContract {
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
pub(crate) struct CompiledTriggerBindings {
    pub(crate) new_row: CompiledTriggerRelation,
    pub(crate) old_row: CompiledTriggerRelation,
    pub(crate) tg_name_slot: usize,
    pub(crate) tg_op_slot: usize,
    pub(crate) tg_when_slot: usize,
    pub(crate) tg_level_slot: usize,
    pub(crate) tg_relid_slot: usize,
    pub(crate) tg_nargs_slot: usize,
    pub(crate) tg_argv_slot: usize,
    pub(crate) tg_table_name_slot: usize,
    pub(crate) tg_table_schema_slot: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledTriggerRelation {
    pub(crate) slots: Vec<usize>,
    pub(crate) field_names: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledEventTriggerBindings {
    pub(crate) tg_event_slot: usize,
    pub(crate) tg_tag_slot: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TriggerReturnedRow {
    New,
    Old,
}

#[derive(Debug, Clone)]
pub(crate) enum CompiledStmt {
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
        targets: Vec<CompiledSelectIntoTarget>,
    },
    ExecUpdate {
        stmt: BoundUpdateStatement,
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

#[derive(Debug, Clone)]
struct ScopeVar {
    slot: usize,
    ty: SqlType,
    constant: bool,
    not_null: bool,
}

#[derive(Debug, Clone)]
struct LabeledScopeVar {
    var: ScopeVar,
    alias: String,
}

#[derive(Debug, Clone)]
struct RelationScopeVar {
    name: String,
    columns: Vec<SlotScopeColumn>,
    trigger_row: Option<TriggerReturnedRow>,
}

#[derive(Debug, Clone)]
struct LabeledScope {
    label: String,
    vars: HashMap<String, LabeledScopeVar>,
    relation_scopes: Vec<RelationScopeVar>,
}

#[derive(Debug, Clone, Default)]
struct CompileEnv {
    vars: HashMap<String, ScopeVar>,
    relation_scopes: Vec<RelationScopeVar>,
    labeled_scopes: Vec<LabeledScope>,
    local_ctes: Vec<BoundCte>,
    declared_cursors: HashMap<String, DeclaredCursor>,
    open_cursor_shapes: HashMap<usize, Vec<QueryColumn>>,
    parameter_slots: Vec<ScopeVar>,
    positional_parameter_names: Vec<String>,
    exception_sqlstate: Option<ScopeVar>,
    exception_sqlerrm: Option<ScopeVar>,
    variable_conflict: PlpgsqlVariableConflict,
    nonstandard_string_literals: bool,
    next_slot: usize,
}

impl CompileEnv {
    fn child(&self) -> Self {
        self.clone()
    }

    fn define_var(&mut self, name: &str, ty: SqlType) -> usize {
        self.define_var_with_options(name, ty, false, false)
    }

    fn define_var_with_options(
        &mut self,
        name: &str,
        ty: SqlType,
        constant: bool,
        not_null: bool,
    ) -> usize {
        let slot = self.allocate_slot();
        self.vars.insert(
            name.to_ascii_lowercase(),
            ScopeVar {
                slot,
                ty,
                constant,
                not_null,
            },
        );
        slot
    }

    fn allocate_slot(&mut self) -> usize {
        let slot = self.next_slot;
        self.next_slot += 1;
        slot
    }

    fn define_exception_slots(&mut self) -> (usize, usize) {
        let text_ty = SqlType::new(SqlTypeKind::Text);
        let sqlstate_slot = self.allocate_slot();
        let sqlerrm_slot = self.allocate_slot();
        self.exception_sqlstate = Some(ScopeVar {
            slot: sqlstate_slot,
            ty: text_ty,
            constant: false,
            not_null: false,
        });
        self.exception_sqlerrm = Some(ScopeVar {
            slot: sqlerrm_slot,
            ty: text_ty,
            constant: false,
            not_null: false,
        });
        (sqlstate_slot, sqlerrm_slot)
    }

    fn with_exception_vars<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, ParseError>,
    ) -> Result<T, ParseError> {
        let saved_sqlstate = self.vars.insert(
            "sqlstate".into(),
            self.exception_sqlstate
                .clone()
                .ok_or(ParseError::UnexpectedEof)?,
        );
        let saved_sqlerrm = self.vars.insert(
            "sqlerrm".into(),
            self.exception_sqlerrm
                .clone()
                .ok_or(ParseError::UnexpectedEof)?,
        );
        let result = f(self);
        restore_optional_var(&mut self.vars, "sqlstate", saved_sqlstate);
        restore_optional_var(&mut self.vars, "sqlerrm", saved_sqlerrm);
        result
    }

    fn define_parameter_var(&mut self, name: &str, ty: SqlType) -> usize {
        let slot = self.define_var(name, ty);
        self.parameter_slots.push(ScopeVar {
            slot,
            ty,
            constant: false,
            not_null: false,
        });
        let positional_name = positional_parameter_var_name(self.parameter_slots.len());
        self.vars.insert(
            positional_name.clone(),
            ScopeVar {
                slot,
                ty,
                constant: false,
                not_null: false,
            },
        );
        self.positional_parameter_names.push(positional_name);
        slot
    }

    fn define_alias(&mut self, name: &str, slot: usize, ty: SqlType) {
        self.vars.insert(
            name.to_ascii_lowercase(),
            ScopeVar {
                slot,
                ty,
                constant: false,
                not_null: false,
            },
        );
    }

    fn update_slot_type(&mut self, slot: usize, ty: SqlType) {
        for var in self.vars.values_mut() {
            if var.slot == slot {
                var.ty = ty;
            }
        }
        for parameter in &mut self.parameter_slots {
            if parameter.slot == slot {
                parameter.ty = ty;
            }
        }
        for scope in &mut self.labeled_scopes {
            for var in scope.vars.values_mut() {
                if var.var.slot == slot {
                    var.var.ty = ty;
                }
            }
        }
    }

    fn get_var(&self, name: &str) -> Option<&ScopeVar> {
        self.vars.get(&name.to_ascii_lowercase())
    }

    fn get_labeled_var(&self, label: &str, name: &str) -> Option<&LabeledScopeVar> {
        self.labeled_scopes
            .iter()
            .rev()
            .find(|scope| scope.label.eq_ignore_ascii_case(label))
            .and_then(|scope| scope.vars.get(&name.to_ascii_lowercase()))
    }

    fn get_labeled_relation_field(
        &self,
        label: &str,
        relation: &str,
        field: &str,
    ) -> Option<&SlotScopeColumn> {
        self.labeled_scopes
            .iter()
            .rev()
            .find(|scope| scope.label.eq_ignore_ascii_case(label))
            .and_then(|scope| {
                scope
                    .relation_scopes
                    .iter()
                    .find(|relation_scope| relation_scope.name.eq_ignore_ascii_case(relation))
            })
            .and_then(|scope| {
                scope
                    .columns
                    .iter()
                    .find(|column| !column.hidden && column.name.eq_ignore_ascii_case(field))
            })
    }

    fn push_label_scope(&mut self, label: &str) {
        let scope_index = self.labeled_scopes.len();
        let captured = self
            .vars
            .iter()
            .filter(|(name, _)| !is_plpgsql_label_alias(name))
            .map(|(name, var)| {
                let alias = plpgsql_label_alias(scope_index, var.slot, name);
                (
                    name.clone(),
                    LabeledScopeVar {
                        var: var.clone(),
                        alias,
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        for var in captured.values() {
            self.vars.insert(var.alias.clone(), var.var.clone());
        }
        self.labeled_scopes.push(LabeledScope {
            label: label.to_ascii_lowercase(),
            vars: captured,
            relation_scopes: self.relation_scopes.clone(),
        });
    }

    fn get_parameter(&self, index: usize) -> Option<&ScopeVar> {
        self.parameter_slots.get(index.saturating_sub(1))
    }

    fn positional_parameter_name(&self, index: usize) -> Option<&str> {
        self.positional_parameter_names
            .get(index.saturating_sub(1))
            .map(String::as_str)
    }

    fn define_relation_scope(
        &mut self,
        name: &str,
        desc: &RelationDesc,
    ) -> CompiledTriggerRelation {
        let mut slots = Vec::with_capacity(desc.columns.len());
        let mut field_names = Vec::with_capacity(desc.columns.len());
        let mut columns = Vec::with_capacity(desc.columns.len());
        for column in &desc.columns {
            let slot = self.next_slot;
            self.next_slot += 1;
            slots.push(slot);
            field_names.push(column.name.clone());
            columns.push(SlotScopeColumn {
                slot,
                name: column.name.clone(),
                sql_type: column.sql_type,
                hidden: column.dropped,
            });
        }
        self.relation_scopes.push(RelationScopeVar {
            name: name.to_ascii_lowercase(),
            columns,
            trigger_row: None,
        });
        CompiledTriggerRelation { slots, field_names }
    }

    fn define_trigger_relation_scope(
        &mut self,
        name: &str,
        desc: &RelationDesc,
        trigger_row: TriggerReturnedRow,
    ) -> CompiledTriggerRelation {
        let relation = self.define_relation_scope(name, desc);
        if let Some(scope) = self
            .relation_scopes
            .iter_mut()
            .find(|scope| scope.name.eq_ignore_ascii_case(name))
        {
            scope.trigger_row = Some(trigger_row);
        }
        relation
    }

    fn define_relation_alias(&mut self, name: &str, target: TriggerReturnedRow) -> bool {
        let source_name = match target {
            TriggerReturnedRow::New => "new",
            TriggerReturnedRow::Old => "old",
        };
        let Some(source) = self
            .relation_scopes
            .iter()
            .find(|scope| scope.name.eq_ignore_ascii_case(source_name))
            .cloned()
        else {
            return false;
        };
        self.relation_scopes.push(RelationScopeVar {
            name: name.to_ascii_lowercase(),
            columns: source.columns,
            trigger_row: Some(target),
        });
        true
    }

    fn get_relation_field(&self, relation: &str, field: &str) -> Option<&SlotScopeColumn> {
        self.relation_scopes
            .iter()
            .find(|scope| scope.name.eq_ignore_ascii_case(relation))
            .and_then(|scope| {
                scope
                    .columns
                    .iter()
                    .find(|column| !column.hidden && column.name.eq_ignore_ascii_case(field))
            })
    }

    fn trigger_relation_return_row(&self, relation: &str) -> Option<TriggerReturnedRow> {
        self.relation_scopes
            .iter()
            .find(|scope| scope.name.eq_ignore_ascii_case(relation))
            .and_then(|scope| scope.trigger_row)
    }

    fn visible_columns(&self) -> Vec<(String, SqlType)> {
        let mut ordered = self
            .vars
            .iter()
            .map(|(name, var)| (var.slot, name.clone(), var.ty))
            .collect::<Vec<_>>();
        ordered.sort_by_key(|(slot, _, _)| *slot);
        ordered
            .into_iter()
            .map(|(_, name, ty)| (name, ty))
            .collect()
    }

    fn define_cursor(
        &mut self,
        name: &str,
        query: &str,
        scrollable: bool,
        params: Vec<DeclaredCursorParam>,
    ) {
        self.declared_cursors.insert(
            name.to_ascii_lowercase(),
            DeclaredCursor {
                query: query.to_string(),
                scrollable,
                params,
            },
        );
    }

    fn declared_cursor(&self, name: &str) -> Option<&DeclaredCursor> {
        self.declared_cursors.get(&name.to_ascii_lowercase())
    }

    fn visible_sql_columns(&self) -> Vec<(String, SqlType)> {
        let mut columns = self.visible_columns();
        columns.extend(
            self.relation_scopes
                .iter()
                .map(|scope| (scope.name.clone(), SqlType::record(RECORD_TYPE_OID))),
        );
        columns
    }

    fn slot_columns(&self) -> Vec<SlotScopeColumn> {
        let mut ordered = self
            .vars
            .iter()
            .map(|(name, var)| SlotScopeColumn {
                slot: var.slot,
                name: name.clone(),
                sql_type: var.ty,
                hidden: false,
            })
            .collect::<Vec<_>>();
        for (name, var) in &self.vars {
            if is_internal_plpgsql_name(name) {
                continue;
            }
            ordered.push(SlotScopeColumn {
                slot: var.slot,
                name: plpgsql_var_alias(var.slot),
                sql_type: var.ty,
                hidden: false,
            });
        }
        ordered.sort_by(|left, right| {
            left.slot
                .cmp(&right.slot)
                .then_with(|| left.name.cmp(&right.name))
        });
        ordered.dedup_by(|left, right| left.name == right.name);
        ordered
    }

    fn relation_slot_scopes(&self) -> Vec<(String, Vec<SlotScopeColumn>)> {
        self.relation_scopes
            .iter()
            .map(|scope| (scope.name.clone(), scope.columns.clone()))
            .collect()
    }
}

fn restore_optional_var(vars: &mut HashMap<String, ScopeVar>, name: &str, saved: Option<ScopeVar>) {
    match saved {
        Some(var) => {
            vars.insert(name.into(), var);
        }
        None => {
            vars.remove(name);
        }
    }
}

pub(crate) fn compile_do_block(
    block: &Block,
    catalog: &dyn CatalogLookup,
) -> Result<CompiledBlock, ParseError> {
    compile_do_block_with_gucs(block, catalog, None)
}

pub(crate) fn compile_do_block_with_gucs(
    block: &Block,
    catalog: &dyn CatalogLookup,
    gucs: Option<&HashMap<String, String>>,
) -> Result<CompiledBlock, ParseError> {
    let mut env = CompileEnv::default();
    env.variable_conflict = variable_conflict_from_gucs(gucs);
    env.nonstandard_string_literals = nonstandard_string_literals_from_gucs(gucs);
    let _ = env.define_var("found", SqlType::new(SqlTypeKind::Bool));
    let _ = env.define_exception_slots();
    compile_block(block, catalog, &mut env, None)
}

pub(crate) fn compile_do_function(
    block: &Block,
    catalog: &dyn CatalogLookup,
    gucs: Option<&HashMap<String, String>>,
) -> Result<CompiledFunction, ParseError> {
    let mut env = CompileEnv::default();
    env.variable_conflict = variable_conflict_from_gucs(gucs);
    env.nonstandard_string_literals = nonstandard_string_literals_from_gucs(gucs);
    let found_slot = env.define_var("found", SqlType::new(SqlTypeKind::Bool));
    let (sqlstate_slot, sqlerrm_slot) = env.define_exception_slots();
    let return_contract = FunctionReturnContract::Scalar {
        ty: SqlType::new(SqlTypeKind::Void),
        setof: false,
        output_slot: None,
    };
    let body = compile_block(block, catalog, &mut env, Some(&return_contract))?;
    Ok(CompiledFunction {
        name: "inline_code_block".into(),
        proc_oid: 0,
        proowner: 0,
        prosecdef: false,
        provolatile: 'v',
        proconfig: None,
        print_strict_params: None,
        parameter_slots: Vec::new(),
        context_arg_type_names: Vec::new(),
        output_slots: Vec::new(),
        body,
        return_contract,
        found_slot,
        sqlstate_slot,
        sqlerrm_slot,
        local_ctes: Vec::new(),
        trigger_transition_ctes: Vec::new(),
    })
}

pub(crate) fn compile_function_from_proc(
    row: &PgProcRow,
    catalog: &dyn CatalogLookup,
    gucs: Option<&HashMap<String, String>>,
) -> Result<CompiledFunction, ParseError> {
    if row.prorettype == EVENT_TRIGGER_TYPE_OID {
        return Err(ParseError::DetailedError {
            message: "trigger functions can only be called as triggers".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    let block = parse_block(&row.prosrc)?;
    let print_strict_params = print_strict_params_directive(&row.prosrc);
    let mut env = CompileEnv::default();
    env.variable_conflict = variable_conflict_mode(&row.prosrc, gucs);
    env.nonstandard_string_literals = nonstandard_string_literals_from_gucs(gucs);
    let mut parameter_slots = Vec::new();
    let mut output_slots = Vec::new();

    let input_type_oids =
        parse_proc_argtype_oids(&row.proargtypes).ok_or_else(|| ParseError::UnexpectedToken {
            expected: "valid pg_proc.proargtypes",
            actual: row.proargtypes.clone(),
        })?;
    let input_types = input_type_oids
        .iter()
        .map(|oid| {
            catalog
                .type_by_oid(*oid)
                .map(|ty| ty.sql_type)
                .ok_or_else(|| ParseError::UnsupportedType(oid.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    if let (Some(all_arg_types), Some(arg_modes)) = (&row.proallargtypes, &row.proargmodes) {
        let arg_names = row.proargnames.clone().unwrap_or_default();
        for (index, (type_oid, mode)) in all_arg_types.iter().zip(arg_modes.iter()).enumerate() {
            let sql_type = catalog
                .type_by_oid(*type_oid)
                .map(|ty| ty.sql_type)
                .ok_or_else(|| ParseError::UnsupportedType(type_oid.to_string()))?;
            let name = arg_names
                .get(index)
                .cloned()
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| format!("column{}", index + 1));
            match *mode {
                b'i' | b'v' => {
                    let slot = env.define_parameter_var(&name, sql_type);
                    parameter_slots.push(CompiledFunctionSlot {
                        name,
                        slot,
                        ty: sql_type,
                    });
                }
                b'b' => {
                    let slot = env.define_parameter_var(&name, sql_type);
                    parameter_slots.push(CompiledFunctionSlot {
                        name: name.clone(),
                        slot,
                        ty: sql_type,
                    });
                    output_slots.push(CompiledOutputSlot {
                        name: name.clone(),
                        slot,
                        column: QueryColumn {
                            name,
                            sql_type,
                            wire_type_oid: None,
                        },
                    });
                }
                b'o' | b't' => {
                    let slot = env.define_parameter_var(&name, sql_type);
                    output_slots.push(CompiledOutputSlot {
                        name: name.clone(),
                        slot,
                        column: QueryColumn {
                            name,
                            sql_type,
                            wire_type_oid: None,
                        },
                    });
                }
                _ => {}
            }
        }
    } else {
        let arg_names = row.proargnames.clone().unwrap_or_default();
        for (index, sql_type) in input_types.into_iter().enumerate() {
            let name = arg_names
                .get(index)
                .cloned()
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| format!("arg{}", index + 1));
            let slot = env.define_parameter_var(&name, sql_type);
            parameter_slots.push(CompiledFunctionSlot {
                name,
                slot,
                ty: sql_type,
            });
        }
    }

    let found_slot = env.define_var("found", SqlType::new(SqlTypeKind::Bool));
    let (sqlstate_slot, sqlerrm_slot) = env.define_exception_slots();
    env.push_label_scope(&row.proname);

    let return_contract = function_return_contract(row, catalog, &output_slots)?;
    let body = compile_block(&block, catalog, &mut env, Some(&return_contract))?;
    let context_arg_type_names = input_type_oids
        .iter()
        .map(|oid| crate::backend::executor::expr_reg::format_type_text(*oid, None, catalog))
        .collect();
    Ok(CompiledFunction {
        name: row.proname.clone(),
        proc_oid: row.oid,
        proowner: row.proowner,
        prosecdef: row.prosecdef,
        provolatile: row.provolatile,
        proconfig: row.proconfig.clone(),
        print_strict_params,
        parameter_slots,
        context_arg_type_names,
        output_slots,
        body,
        return_contract,
        found_slot,
        sqlstate_slot,
        sqlerrm_slot,
        local_ctes: Vec::new(),
        trigger_transition_ctes: Vec::new(),
    })
}

fn print_strict_params_directive(source: &str) -> Option<bool> {
    source.lines().find_map(|line| {
        let line = line.trim();
        let rest = line.strip_prefix("#print_strict_params")?.trim();
        if rest.eq_ignore_ascii_case("on") {
            Some(true)
        } else if rest.eq_ignore_ascii_case("off") {
            Some(false)
        } else {
            None
        }
    })
}

fn variable_conflict_mode(
    source: &str,
    gucs: Option<&HashMap<String, String>>,
) -> PlpgsqlVariableConflict {
    variable_conflict_directive(source).unwrap_or_else(|| variable_conflict_from_gucs(gucs))
}

fn variable_conflict_from_gucs(gucs: Option<&HashMap<String, String>>) -> PlpgsqlVariableConflict {
    gucs.and_then(|gucs| gucs.get("plpgsql.variable_conflict"))
        .and_then(|value| parse_variable_conflict_mode(value))
        .unwrap_or_default()
}

fn nonstandard_string_literals_from_gucs(gucs: Option<&HashMap<String, String>>) -> bool {
    gucs.and_then(|gucs| gucs.get("standard_conforming_strings"))
        .is_some_and(|value| value.eq_ignore_ascii_case("off"))
}

fn variable_conflict_directive(source: &str) -> Option<PlpgsqlVariableConflict> {
    source.lines().find_map(|line| {
        let line = line.trim();
        let rest = line.strip_prefix("#variable_conflict")?.trim();
        rest.split_whitespace()
            .next()
            .and_then(parse_variable_conflict_mode)
    })
}

fn parse_variable_conflict_mode(value: &str) -> Option<PlpgsqlVariableConflict> {
    match value.trim().to_ascii_lowercase().as_str() {
        "error" => Some(PlpgsqlVariableConflict::Error),
        "use_variable" => Some(PlpgsqlVariableConflict::UseVariable),
        "use_column" => Some(PlpgsqlVariableConflict::UseColumn),
        _ => None,
    }
}

pub(crate) fn compile_trigger_function_from_proc(
    row: &PgProcRow,
    relation_desc: &RelationDesc,
    transition_tables: &[TriggerTransitionTable],
    catalog: &dyn CatalogLookup,
    gucs: Option<&HashMap<String, String>>,
) -> Result<CompiledFunction, ParseError> {
    let block = parse_block(&row.prosrc)?;
    let print_strict_params = print_strict_params_directive(&row.prosrc);
    let mut env = CompileEnv::default();
    env.variable_conflict = variable_conflict_mode(&row.prosrc, gucs);
    env.nonstandard_string_literals = nonstandard_string_literals_from_gucs(gucs);
    let mut trigger_transition_ctes = Vec::new();
    env.local_ctes = transition_tables
        .iter()
        .map(|table| {
            let cte = crate::backend::parser::bound_cte_from_materialized_rows(
                table.name.clone(),
                &table.desc,
                &[],
            );
            trigger_transition_ctes.push(CompiledTriggerTransitionCte {
                name: table.name.clone(),
                cte_id: cte.cte_id,
            });
            cte
        })
        .collect();
    let bindings = seed_trigger_env(&mut env, relation_desc);
    let found_slot = env.define_var("found", SqlType::new(SqlTypeKind::Bool));
    let (sqlstate_slot, sqlerrm_slot) = env.define_exception_slots();
    env.push_label_scope(&row.proname);
    let return_contract = FunctionReturnContract::Trigger { bindings };
    let body = compile_block(&block, catalog, &mut env, Some(&return_contract))?;
    Ok(CompiledFunction {
        name: row.proname.clone(),
        proc_oid: row.oid,
        proowner: row.proowner,
        prosecdef: row.prosecdef,
        provolatile: row.provolatile,
        proconfig: row.proconfig.clone(),
        print_strict_params,
        parameter_slots: Vec::new(),
        context_arg_type_names: Vec::new(),
        output_slots: Vec::new(),
        body,
        return_contract,
        found_slot,
        sqlstate_slot,
        sqlerrm_slot,
        local_ctes: env.local_ctes.clone(),
        trigger_transition_ctes,
    })
}

pub(crate) fn compile_event_trigger_function_from_proc(
    row: &PgProcRow,
    catalog: &dyn CatalogLookup,
) -> Result<CompiledFunction, ParseError> {
    let block = parse_block(&row.prosrc)?;
    let mut env = CompileEnv::default();
    let bindings = seed_event_trigger_env(&mut env);
    let found_slot = env.define_var("found", SqlType::new(SqlTypeKind::Bool));
    let (sqlstate_slot, sqlerrm_slot) = env.define_exception_slots();
    let return_contract = FunctionReturnContract::EventTrigger { bindings };
    let body = compile_block(&block, catalog, &mut env, Some(&return_contract))?;
    Ok(CompiledFunction {
        name: row.proname.clone(),
        proc_oid: row.oid,
        proowner: row.proowner,
        prosecdef: row.prosecdef,
        provolatile: row.provolatile,
        proconfig: row.proconfig.clone(),
        print_strict_params: None,
        parameter_slots: Vec::new(),
        context_arg_type_names: Vec::new(),
        output_slots: Vec::new(),
        body,
        return_contract,
        found_slot,
        sqlstate_slot,
        sqlerrm_slot,
        local_ctes: Vec::new(),
        trigger_transition_ctes: Vec::new(),
    })
}

fn function_return_contract(
    row: &PgProcRow,
    catalog: &dyn CatalogLookup,
    output_slots: &[CompiledOutputSlot],
) -> Result<FunctionReturnContract, ParseError> {
    if row.prokind == 'p' {
        return if output_slots.is_empty() {
            Ok(FunctionReturnContract::Scalar {
                ty: SqlType::new(SqlTypeKind::Void),
                setof: false,
                output_slot: None,
            })
        } else {
            Ok(FunctionReturnContract::FixedRow {
                columns: output_slots
                    .iter()
                    .map(|slot| slot.column.clone())
                    .collect(),
                setof: false,
                uses_output_vars: true,
                composite_typrelid: None,
            })
        };
    }

    let result_type = catalog
        .type_by_oid(row.prorettype)
        .map(|ty| ty.sql_type)
        .unwrap_or_else(|| SqlType::record(RECORD_TYPE_OID));
    if row.proretset {
        return Ok(match result_type.kind {
            SqlTypeKind::Record => {
                if output_slots.is_empty() {
                    FunctionReturnContract::AnonymousRecord { setof: true }
                } else {
                    FunctionReturnContract::FixedRow {
                        columns: output_slots
                            .iter()
                            .map(|slot| slot.column.clone())
                            .collect(),
                        setof: true,
                        uses_output_vars: true,
                        composite_typrelid: None,
                    }
                }
            }
            SqlTypeKind::Composite => {
                let relation = catalog
                    .lookup_relation_by_oid(result_type.typrelid)
                    .ok_or_else(|| ParseError::UnsupportedType(result_type.typrelid.to_string()))?;
                FunctionReturnContract::FixedRow {
                    columns: relation
                        .desc
                        .columns
                        .into_iter()
                        .filter(|column| !column.dropped)
                        .map(|column| QueryColumn {
                            name: column.name,
                            sql_type: column.sql_type,
                            wire_type_oid: None,
                        })
                        .collect(),
                    setof: true,
                    uses_output_vars: false,
                    composite_typrelid: Some(result_type.typrelid),
                }
            }
            _ => FunctionReturnContract::Scalar {
                ty: result_type,
                setof: true,
                output_slot: output_slots.first().map(|slot| slot.slot),
            },
        });
    }

    match result_type.kind {
        SqlTypeKind::Trigger => Err(ParseError::FeatureNotSupported(
            "trigger functions cannot be called in SQL expressions".into(),
        )),
        SqlTypeKind::Record if !output_slots.is_empty() => Ok(FunctionReturnContract::FixedRow {
            columns: output_slots
                .iter()
                .map(|slot| slot.column.clone())
                .collect(),
            setof: false,
            uses_output_vars: true,
            composite_typrelid: None,
        }),
        SqlTypeKind::Record => Ok(FunctionReturnContract::AnonymousRecord { setof: false }),
        SqlTypeKind::Composite => {
            let relation = catalog
                .lookup_relation_by_oid(result_type.typrelid)
                .ok_or_else(|| ParseError::UnsupportedType(result_type.typrelid.to_string()))?;
            Ok(FunctionReturnContract::FixedRow {
                columns: relation
                    .desc
                    .columns
                    .into_iter()
                    .filter(|column| !column.dropped)
                    .map(|column| QueryColumn {
                        name: column.name,
                        sql_type: column.sql_type,
                        wire_type_oid: None,
                    })
                    .collect(),
                setof: false,
                uses_output_vars: false,
                composite_typrelid: Some(result_type.typrelid),
            })
        }
        _ => Ok(FunctionReturnContract::Scalar {
            ty: result_type,
            setof: false,
            output_slot: output_slots.first().map(|slot| slot.slot),
        }),
    }
}

fn compile_block(
    block: &Block,
    catalog: &dyn CatalogLookup,
    outer: &mut CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<CompiledBlock, ParseError> {
    let mut env = outer.child();
    let mut local_slots = Vec::new();
    for decl in &block.declarations {
        match decl {
            Decl::Var(decl) => local_slots.push(compile_var_decl(decl, catalog, &mut env)?),
            Decl::Cursor(decl) => local_slots.push(compile_cursor_decl(decl, catalog, &mut env)?),
            Decl::Alias(decl) => compile_alias_decl(decl, &mut env)?,
        }
    }
    if let Some(label) = &block.label {
        env.push_label_scope(label);
    }
    let statements = block
        .statements
        .iter()
        .map(|stmt| compile_stmt(stmt, catalog, &mut env, return_contract))
        .collect::<Result<Vec<_>, _>>()?;
    let exception_handlers = block
        .exception_handlers
        .iter()
        .map(|handler| {
            let statements = env.with_exception_vars(|handler_env| {
                compile_stmt_list(&handler.statements, catalog, handler_env, return_contract)
            })?;
            Ok(CompiledExceptionHandler {
                conditions: handler.conditions.clone(),
                statements,
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    outer.next_slot = outer.next_slot.max(env.next_slot);
    Ok(CompiledBlock {
        local_slots,
        statements,
        exception_handlers,
        exception_sqlstate_slot: env.exception_sqlstate.as_ref().map(|var| var.slot),
        exception_sqlerrm_slot: env.exception_sqlerrm.as_ref().map(|var| var.slot),
        total_slots: outer.next_slot,
    })
}

fn compile_var_decl(
    decl: &VarDecl,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledVar, ParseError> {
    let ty = resolve_decl_type(&decl.type_name, catalog, env)?;
    let default_expr = decl
        .default_expr
        .as_deref()
        .map(|expr| compile_assignment_expr_text(expr, catalog, env))
        .transpose()?;
    let slot = env.define_var_with_options(&decl.name, ty, decl.constant, decl.strict);
    Ok(CompiledVar {
        name: decl.name.clone(),
        slot,
        ty,
        default_expr,
        not_null: decl.strict,
        line: decl.line,
    })
}

fn compile_cursor_decl(
    decl: &CursorDecl,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledVar, ParseError> {
    let ty = SqlType::new(SqlTypeKind::Text)
        .with_identity(crate::include::catalog::REFCURSOR_TYPE_OID, 0);
    let slot = env.define_var(&decl.name, ty);
    env.define_cursor(
        &decl.name,
        &decl.query,
        decl.scrollable,
        decl.params
            .iter()
            .map(|param| DeclaredCursorParam {
                name: param.name.clone(),
                type_name: param.type_name.clone(),
                ty: param.ty,
            })
            .collect(),
    );
    Ok(CompiledVar {
        name: decl.name.clone(),
        slot,
        ty,
        default_expr: Some(compile_expr_text(
            &format!("'{}'", decl.name.replace('\'', "''")),
            catalog,
            env,
        )?),
        not_null: false,
        line: 1,
    })
}

fn resolve_decl_type(
    type_name: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<SqlType, ParseError> {
    let trimmed = type_name.trim();
    let lowered = trimmed.to_ascii_lowercase();
    if let Some(prefix) = lowered.strip_suffix("%type") {
        let original_prefix = &trimmed[..prefix.len()];
        if !original_prefix.contains('.')
            && let Some(var) = env.get_var(original_prefix.trim())
        {
            return Ok(var.ty);
        }
        let Some((relation_name, column_name)) = original_prefix.trim().rsplit_once('.') else {
            return Err(ParseError::UnexpectedToken {
                expected: "PL/pgSQL %TYPE reference in relation.column form",
                actual: type_name.into(),
            });
        };
        let relation = catalog
            .lookup_any_relation(relation_name.trim())
            .ok_or_else(|| ParseError::UnsupportedType(relation_name.trim().into()))?;
        return relation
            .desc
            .columns
            .iter()
            .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name.trim()))
            .map(|column| column.sql_type)
            .ok_or_else(|| ParseError::UnknownColumn(original_prefix.trim().into()));
    }
    if let Some(prefix) = lowered.strip_suffix("%rowtype") {
        let relation_name = &trimmed[..prefix.len()];
        let relation = catalog
            .lookup_any_relation(relation_name.trim())
            .ok_or_else(|| ParseError::UnsupportedType(relation_name.trim().into()))?;
        return Ok(relation_row_type(&relation, catalog));
    }
    resolve_raw_type_name(&parse_type_name(trimmed)?, catalog)
}

fn relation_row_type(
    relation: &crate::backend::parser::BoundRelation,
    catalog: &dyn CatalogLookup,
) -> SqlType {
    catalog
        .type_rows()
        .into_iter()
        .find(|row| row.typrelid == relation.relation_oid)
        .map(|row| SqlType::named_composite(row.oid, relation.relation_oid))
        .unwrap_or_else(|| SqlType::record(RECORD_TYPE_OID))
}

fn compile_alias_decl(
    decl: &super::ast::AliasDecl,
    env: &mut CompileEnv,
) -> Result<(), ParseError> {
    match decl.target {
        AliasTarget::Parameter(index) => {
            let parameter =
                env.get_parameter(index)
                    .cloned()
                    .ok_or_else(|| ParseError::UnexpectedToken {
                        expected: "function parameter referenced by ALIAS FOR",
                        actual: format!("${index}"),
                    })?;
            env.define_alias(&decl.name, parameter.slot, parameter.ty);
        }
        AliasTarget::New => {
            if !env.define_relation_alias(&decl.name, TriggerReturnedRow::New) {
                return Err(ParseError::UnexpectedToken {
                    expected: "trigger NEW row available for ALIAS FOR",
                    actual: "NEW".into(),
                });
            }
        }
        AliasTarget::Old => {
            if !env.define_relation_alias(&decl.name, TriggerReturnedRow::Old) {
                return Err(ParseError::UnexpectedToken {
                    expected: "trigger OLD row available for ALIAS FOR",
                    actual: "OLD".into(),
                });
            }
        }
    }
    Ok(())
}

fn compile_stmt(
    stmt: &Stmt,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<CompiledStmt, ParseError> {
    Ok(match stmt {
        Stmt::WithLine { line, stmt } => CompiledStmt::WithLine {
            line: *line,
            stmt: Box::new(compile_stmt(stmt, catalog, env, return_contract)?),
        },
        Stmt::Block(block) => {
            CompiledStmt::Block(compile_block(block, catalog, env, return_contract)?)
        }
        Stmt::Assign { target, expr, line } => {
            if let Some(target) = compile_indirect_assign_target(target, catalog, env)? {
                CompiledStmt::AssignIndirect {
                    target,
                    expr: compile_assignment_expr_text(expr, catalog, env)?,
                    line: *line,
                }
            } else if let AssignTarget::Subscript { name, subscripts } = target {
                let (slot, root_ty, _, _) =
                    resolve_assign_target(&AssignTarget::Name(name.clone()), env)?;
                CompiledStmt::AssignSubscript {
                    slot,
                    root_ty,
                    target_ty: subscripted_assignment_target_type(
                        root_ty,
                        subscripts.len(),
                        catalog,
                    )?,
                    subscripts: subscripts
                        .iter()
                        .map(|subscript| compile_expr_text(subscript, catalog, env))
                        .collect::<Result<Vec<_>, _>>()?,
                    expr: compile_assignment_expr_text(expr, catalog, env)?,
                    line: *line,
                }
            } else {
                let (slot, ty, name, not_null) = resolve_assign_target(target, env)?;
                CompiledStmt::Assign {
                    slot,
                    ty,
                    name,
                    not_null,
                    expr: compile_assignment_expr_text(expr, catalog, env)?,
                    line: *line,
                }
            }
        }
        Stmt::Null => CompiledStmt::Null,
        Stmt::If {
            branches,
            else_branch,
        } => CompiledStmt::If {
            branches: branches
                .iter()
                .map(|(condition, body)| {
                    Ok((
                        compile_condition_text(condition, catalog, env)?,
                        compile_stmt_list(body, catalog, env, return_contract)?,
                    ))
                })
                .collect::<Result<_, ParseError>>()?,
            else_branch: compile_stmt_list(else_branch, catalog, env, return_contract)?,
        },
        Stmt::While { condition, body } => CompiledStmt::While {
            condition: compile_condition_text(condition, catalog, env)?,
            body: compile_stmt_list(body, catalog, env, return_contract)?,
        },
        Stmt::Loop { body } => CompiledStmt::Loop {
            body: compile_stmt_list(body, catalog, env, return_contract)?,
        },
        Stmt::Exit { condition } => CompiledStmt::Exit {
            condition: condition
                .as_deref()
                .map(|condition| compile_condition_text(condition, catalog, env))
                .transpose()?,
        },
        Stmt::ForInt {
            var_name,
            start_expr,
            end_expr,
            body,
        } => {
            let mut loop_env = env.child();
            let slot = loop_env.define_var(var_name, SqlType::new(SqlTypeKind::Int4));
            let body = compile_stmt_list(body, catalog, &mut loop_env, return_contract)?;
            env.next_slot = env.next_slot.max(loop_env.next_slot);
            CompiledStmt::ForInt {
                slot,
                start_expr: compile_expr_text(start_expr, catalog, env)?,
                end_expr: compile_expr_text(end_expr, catalog, env)?,
                body,
            }
        }
        Stmt::ForQuery {
            target,
            source,
            body,
        } => compile_for_query_stmt(target, source, body, catalog, env, return_contract)?,
        Stmt::ForEach {
            target,
            slice,
            array_expr,
            body,
        } => compile_foreach_stmt(
            target,
            *slice,
            array_expr,
            body,
            catalog,
            env,
            return_contract,
        )?,
        Stmt::Raise {
            level,
            condition,
            message,
            params,
            using_options,
        } => compile_raise_stmt(
            level,
            condition,
            message,
            params,
            using_options,
            catalog,
            env,
        )?,
        Stmt::Assert { condition, message } => CompiledStmt::Assert {
            condition: compile_condition_text(condition, catalog, env)?,
            message: message
                .as_deref()
                .map(|expr| compile_expr_text(expr, catalog, env))
                .transpose()?,
        },
        Stmt::Continue { condition } => CompiledStmt::Continue {
            condition: condition
                .as_deref()
                .map(|expr| compile_condition_text(expr, catalog, env))
                .transpose()?,
        },
        Stmt::Return { expr, line } => {
            compile_return_stmt(expr.as_deref(), *line, catalog, env, return_contract)?
        }
        Stmt::ReturnNext { expr } => {
            compile_return_next_stmt(expr.as_deref(), catalog, env, return_contract)?
        }
        Stmt::ReturnQuery { source } => {
            compile_return_query_stmt(source, catalog, env, return_contract)?
        }
        Stmt::Perform { sql, line } => compile_perform_stmt(sql, *line, catalog, env)?,
        Stmt::DynamicExecute {
            sql_expr,
            strict,
            into_targets,
            using_exprs,
            line,
        } => compile_dynamic_execute_stmt(
            sql_expr,
            *strict,
            into_targets,
            using_exprs,
            *line,
            catalog,
            env,
        )?,
        Stmt::GetDiagnostics { stacked, items } => {
            let items = items
                .iter()
                .map(|(target, item)| Ok((compile_select_into_target(target, env)?, item.clone())))
                .collect::<Result<Vec<_>, ParseError>>()?;
            CompiledStmt::GetDiagnostics {
                stacked: *stacked,
                items,
            }
        }
        Stmt::OpenCursor { name, source } => compile_open_cursor_stmt(name, source, catalog, env)?,
        Stmt::FetchCursor {
            name,
            direction,
            targets,
        } => {
            let (slot, _, _, _) = resolve_assign_target(&AssignTarget::Name(name.clone()), env)?;
            let cursor_shape = env.open_cursor_shapes.get(&slot).cloned();
            let mut targets = targets
                .iter()
                .map(|target| compile_select_into_target(target, env))
                .collect::<Result<Vec<_>, _>>()?;
            apply_cursor_shape_to_fetch_targets(&mut targets, cursor_shape.as_deref(), env);
            CompiledStmt::FetchCursor {
                slot,
                direction: *direction,
                targets,
            }
        }
        Stmt::MoveCursor { name, direction } => {
            let (slot, _, _, _) = resolve_assign_target(&AssignTarget::Name(name.clone()), env)?;
            CompiledStmt::MoveCursor {
                slot,
                direction: *direction,
            }
        }
        Stmt::CloseCursor { name } => {
            let (slot, _, _, _) = resolve_assign_target(&AssignTarget::Name(name.clone()), env)?;
            CompiledStmt::CloseCursor { slot }
        }
        Stmt::ExecSql { sql } => compile_exec_sql_stmt(sql, catalog, env)?,
    })
}

fn compile_raise_stmt(
    level: &RaiseLevel,
    condition: &Option<RaiseCondition>,
    message: &Option<String>,
    params: &[String],
    using_options: &[RaiseUsingOption],
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    if condition.is_none() && message.is_none() && params.is_empty() && using_options.is_empty() {
        return Ok(CompiledStmt::Reraise);
    }

    let mut sqlstate = None::<String>;
    let mut default_message = None::<String>;
    let condition_sets_errcode = condition.is_some();
    match condition {
        Some(RaiseCondition::SqlState(value)) => {
            sqlstate = Some(value.clone());
            default_message = Some(value.clone());
        }
        Some(RaiseCondition::ConditionName(name)) => {
            sqlstate = Some(
                exception_condition_name_sqlstate(name)
                    .unwrap_or("P0001")
                    .to_string(),
            );
            default_message = Some(name.clone());
        }
        None => {}
    }

    let mut message_expr = None::<String>;
    let mut detail_expr = None::<String>;
    let mut hint_expr = None::<String>;
    let mut errcode_expr = None::<String>;
    let mut column_expr = None::<String>;
    let mut constraint_expr = None::<String>;
    let mut datatype_expr = None::<String>;
    let mut table_expr = None::<String>;
    let mut schema_expr = None::<String>;
    for option in using_options {
        match option.name.to_ascii_lowercase().as_str() {
            "message" => {
                if message.is_some() || message_expr.is_some() {
                    return duplicate_raise_option("MESSAGE");
                }
                message_expr = Some(option.expr.clone());
            }
            "detail" => {
                if detail_expr.is_some() {
                    return duplicate_raise_option("DETAIL");
                }
                detail_expr = Some(option.expr.clone());
            }
            "hint" => {
                if hint_expr.is_some() {
                    return duplicate_raise_option("HINT");
                }
                hint_expr = Some(option.expr.clone());
            }
            "errcode" => {
                if condition_sets_errcode || errcode_expr.is_some() {
                    return duplicate_raise_option("ERRCODE");
                }
                errcode_expr = Some(option.expr.clone());
            }
            "column" | "column_name" => {
                if column_expr.is_some() {
                    return duplicate_raise_option("COLUMN");
                }
                column_expr = Some(option.expr.clone());
            }
            "constraint" | "constraint_name" => {
                if constraint_expr.is_some() {
                    return duplicate_raise_option("CONSTRAINT");
                }
                constraint_expr = Some(option.expr.clone());
            }
            "datatype" | "datatype_name" => {
                if datatype_expr.is_some() {
                    return duplicate_raise_option("DATATYPE");
                }
                datatype_expr = Some(option.expr.clone());
            }
            "table" | "table_name" => {
                if table_expr.is_some() {
                    return duplicate_raise_option("TABLE");
                }
                table_expr = Some(option.expr.clone());
            }
            "schema" | "schema_name" => {
                if schema_expr.is_some() {
                    return duplicate_raise_option("SCHEMA");
                }
                schema_expr = Some(option.expr.clone());
            }
            _ => {}
        }
    }

    let message = message
        .as_ref()
        .map(|message| {
            if env.nonstandard_string_literals {
                decode_nonstandard_backslash_escapes(message)
            } else {
                message.clone()
            }
        })
        .or(default_message)
        .or_else(|| {
            if message_expr.is_none() {
                Some(sqlstate.clone().unwrap_or_else(|| "P0001".into()))
            } else {
                None
            }
        });

    if let Some(message) = &message {
        let placeholder_count = count_raise_placeholders(message);
        if placeholder_count != params.len() {
            return Err(ParseError::UnexpectedToken {
                expected: "RAISE placeholder count matching argument count",
                actual: format!(
                    "message has {placeholder_count} placeholders but {} arguments were provided",
                    params.len()
                ),
            });
        }
    } else if !params.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "RAISE format string before parameter list",
            actual: format!("{params:?}"),
        });
    }

    Ok(CompiledStmt::Raise {
        line: 1,
        level: level.clone(),
        sqlstate,
        message,
        message_expr: message_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        detail_expr: detail_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        hint_expr: hint_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        errcode_expr: errcode_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        column_expr: column_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        constraint_expr: constraint_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        datatype_expr: datatype_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        table_expr: table_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        schema_expr: schema_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        params: params
            .iter()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .collect::<Result<_, _>>()?,
    })
}

fn duplicate_raise_option<T>(name: &str) -> Result<T, ParseError> {
    Err(ParseError::UnexpectedToken {
        expected: "RAISE option specified once",
        actual: format!("RAISE option already specified: {name}"),
    })
}

fn count_raise_placeholders(message: &str) -> usize {
    let mut count = 0usize;
    let mut chars = message.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '%' {
            if chars.peek() == Some(&'%') {
                chars.next();
            } else {
                count += 1;
            }
        }
    }
    count
}

fn exception_condition_name_sqlstate(name: &str) -> Option<&'static str> {
    match name.to_ascii_lowercase().as_str() {
        "assert_failure" => Some("P0004"),
        "data_corrupted" => Some("XX001"),
        "division_by_zero" => Some("22012"),
        "feature_not_supported" => Some("0A000"),
        "raise_exception" => Some("P0001"),
        "reading_sql_data_not_permitted" => Some("2F003"),
        "syntax_error" => Some("42601"),
        "no_data_found" => Some("P0002"),
        "too_many_rows" => Some("P0003"),
        "unique_violation" => Some("23505"),
        "not_null_violation" => Some("23502"),
        "check_violation" => Some("23514"),
        "foreign_key_violation" => Some("23503"),
        "undefined_file" => Some("58P01"),
        "invalid_parameter_value" => Some("22023"),
        "null_value_not_allowed" => Some("22004"),
        _ => None,
    }
}

fn compile_return_stmt(
    expr: Option<&str>,
    line: usize,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<CompiledStmt, ParseError> {
    let Some(contract) = return_contract else {
        return Err(ParseError::FeatureNotSupported(
            "RETURN is only supported inside CREATE FUNCTION".into(),
        ));
    };
    match (contract, expr) {
        (FunctionReturnContract::Trigger { .. }, Some(expr))
            if env.trigger_relation_return_row(expr.trim()).is_some() =>
        {
            Ok(CompiledStmt::ReturnTriggerRow {
                row: env
                    .trigger_relation_return_row(expr.trim())
                    .ok_or(ParseError::UnexpectedEof)?,
            })
        }
        (FunctionReturnContract::Trigger { .. }, Some(expr))
            if expr.trim().eq_ignore_ascii_case("null") =>
        {
            Ok(CompiledStmt::ReturnTriggerNull)
        }
        (FunctionReturnContract::Trigger { .. }, None) => Ok(CompiledStmt::ReturnTriggerNoValue),
        (FunctionReturnContract::Trigger { .. }, Some(_)) => Err(ParseError::FeatureNotSupported(
            "trigger RETURN expressions must be NEW, OLD, or NULL".into(),
        )),
        (FunctionReturnContract::EventTrigger { .. }, None) => {
            Ok(CompiledStmt::ReturnTriggerNoValue)
        }
        (FunctionReturnContract::EventTrigger { .. }, Some(_)) => Err(ParseError::DetailedError {
            message: "RETURN cannot have a parameter in function returning event_trigger".into(),
            detail: None,
            hint: None,
            sqlstate: "42804",
        }),
        (
            FunctionReturnContract::Scalar {
                output_slot: Some(_),
                ..
            }
            | FunctionReturnContract::FixedRow {
                uses_output_vars: true,
                ..
            },
            Some(_),
        ) => Err(ParseError::DetailedError {
            message: "RETURN cannot have a parameter in function with OUT parameters".into(),
            detail: None,
            hint: None,
            sqlstate: "42804",
        }),
        (
            FunctionReturnContract::Scalar {
                ty,
                output_slot: None,
                setof: false,
            },
            Some(_),
        ) if ty.kind == SqlTypeKind::Void => Err(ParseError::DetailedError {
            message: "RETURN cannot have a parameter in function returning void".into(),
            detail: None,
            hint: None,
            sqlstate: "42804",
        }),
        (FunctionReturnContract::Scalar { setof: false, .. }, Some(expr)) => {
            if let Some((sql, plan)) = compile_return_select_expr(expr, catalog, env)? {
                return Ok(CompiledStmt::ReturnSelect { plan, sql, line });
            }
            if let Some(sql) = runtime_return_query_sql(expr, env)? {
                return Ok(CompiledStmt::ReturnRuntimeQuery {
                    sql,
                    scope: runtime_sql_scope(env),
                    line,
                });
            }
            Ok(CompiledStmt::Return {
                expr: Some(compile_expr_text(expr, catalog, env)?),
                line,
            })
        }
        (
            FunctionReturnContract::Scalar {
                ty,
                output_slot,
                setof,
                ..
            },
            None,
        ) if output_slot.is_some() || *setof || ty.kind == SqlTypeKind::Void => {
            Ok(CompiledStmt::Return { expr: None, line })
        }
        (FunctionReturnContract::FixedRow { .. }, None)
        | (FunctionReturnContract::AnonymousRecord { .. }, None) => {
            Ok(CompiledStmt::Return { expr: None, line })
        }
        (
            FunctionReturnContract::FixedRow { setof: false, .. }
            | FunctionReturnContract::AnonymousRecord { setof: false },
            Some(expr),
        ) => Ok(CompiledStmt::Return {
            expr: Some(compile_expr_text(expr, catalog, env)?),
            line,
        }),
        _ => Err(ParseError::FeatureNotSupported(
            "RETURN expr is only supported for scalar function returns".into(),
        )),
    }
}

fn runtime_return_query_sql(expr: &str, env: &CompileEnv) -> Result<Option<String>, ParseError> {
    let Some(from_idx) = find_keyword_at_top_level(expr, "from") else {
        return Ok(None);
    };
    let before_from = expr[..from_idx].trim();
    let after_from = expr[from_idx..].trim();
    if before_from.is_empty() || after_from.is_empty() {
        return Ok(None);
    }
    Ok(Some(rewrite_plpgsql_sql_text(
        &format!("select {before_from} {after_from}"),
        env,
    )?))
}

fn compile_return_next_stmt(
    expr: Option<&str>,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<CompiledStmt, ParseError> {
    let Some(contract) = return_contract else {
        return Err(ParseError::FeatureNotSupported(
            "RETURN NEXT is only supported inside CREATE FUNCTION".into(),
        ));
    };
    match (contract, expr) {
        (FunctionReturnContract::Trigger { .. }, _)
        | (FunctionReturnContract::EventTrigger { .. }, _) => Err(ParseError::FeatureNotSupported(
            "RETURN NEXT is not valid in trigger functions".into(),
        )),
        (FunctionReturnContract::Scalar { setof: true, .. }, Some(expr)) => {
            Ok(CompiledStmt::ReturnNext {
                expr: Some(compile_expr_text(expr, catalog, env)?),
            })
        }
        (FunctionReturnContract::FixedRow { setof: true, .. }, Some(expr))
        | (FunctionReturnContract::AnonymousRecord { setof: true }, Some(expr)) => {
            Ok(CompiledStmt::ReturnNext {
                expr: Some(compile_expr_text(expr, catalog, env)?),
            })
        }
        (
            FunctionReturnContract::Scalar {
                setof: true,
                output_slot: Some(_),
                ..
            },
            None,
        ) => Ok(CompiledStmt::ReturnNext { expr: None }),
        (
            FunctionReturnContract::FixedRow {
                setof: true,
                uses_output_vars: true,
                ..
            },
            None,
        ) => Ok(CompiledStmt::ReturnNext { expr: None }),
        _ => Err(ParseError::FeatureNotSupported(
            "RETURN NEXT is not valid for this function return contract".into(),
        )),
    }
}

fn plan_select_for_env(
    stmt: &crate::backend::parser::SelectStatement,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<PlannedStmt, ParseError> {
    validate_select_variable_conflicts(stmt, catalog, env)?;
    let stmt = normalize_plpgsql_select(stmt.clone(), env);
    pg_plan_query_with_outer_scopes_and_ctes_config(
        &stmt,
        catalog,
        &[outer_scope_for_sql(env)],
        &env.local_ctes,
        plpgsql_planner_config(),
    )
}

fn plan_values_for_env(
    stmt: &crate::backend::parser::ValuesStatement,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<PlannedStmt, ParseError> {
    let stmt = normalize_plpgsql_values(stmt.clone(), env);
    pg_plan_values_query_with_outer_scopes_and_ctes_config(
        &stmt,
        catalog,
        &[outer_scope_for_sql(env)],
        &env.local_ctes,
        plpgsql_planner_config(),
    )
}

fn plpgsql_planner_config() -> PlannerConfig {
    PlannerConfig {
        fold_constants: false,
        ..PlannerConfig::default()
    }
}

fn validate_select_variable_conflicts(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<(), ParseError> {
    if env.variable_conflict != PlpgsqlVariableConflict::Error {
        return Ok(());
    }
    let Some(from) = stmt.from.as_ref() else {
        return Ok(());
    };
    let mut from_columns = HashSet::new();
    collect_from_item_column_names(from, catalog, env, &mut from_columns);
    if from_columns.is_empty() {
        return Ok(());
    }
    let mut refs = Vec::new();
    collect_select_column_refs(stmt, &mut refs);
    for name in refs {
        if from_columns.contains(&name.to_ascii_lowercase()) && env.get_var(&name).is_some() {
            return Err(ambiguous_plpgsql_column_error(&name));
        }
    }
    Ok(())
}

fn ambiguous_plpgsql_column_error(name: &str) -> ParseError {
    ParseError::DetailedError {
        message: format!("column reference \"{name}\" is ambiguous"),
        detail: Some("It could refer to either a PL/pgSQL variable or a table column.".into()),
        hint: None,
        sqlstate: "42702",
    }
}

fn collect_from_item_column_names(
    item: &FromItem,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
    names: &mut HashSet<String>,
) {
    match item {
        FromItem::Table { name, .. } => {
            if let Some(cte) = env
                .local_ctes
                .iter()
                .find(|cte| cte.name.eq_ignore_ascii_case(name))
            {
                names.extend(
                    cte.desc
                        .columns
                        .iter()
                        .filter(|column| !column.dropped)
                        .map(|column| column.name.to_ascii_lowercase()),
                );
                return;
            }
            if let Some(relation) = catalog.lookup_any_relation(name) {
                names.extend(
                    relation
                        .desc
                        .columns
                        .iter()
                        .filter(|column| !column.dropped)
                        .map(|column| column.name.to_ascii_lowercase()),
                );
            }
        }
        FromItem::Alias {
            source,
            column_aliases,
            ..
        } => match column_aliases {
            AliasColumnSpec::Names(alias_names) if !alias_names.is_empty() => {
                names.extend(alias_names.iter().map(|name| name.to_ascii_lowercase()));
            }
            AliasColumnSpec::Definitions(defs) if !defs.is_empty() => {
                names.extend(defs.iter().map(|def| def.name.to_ascii_lowercase()));
            }
            _ => collect_from_item_column_names(source, catalog, env, names),
        },
        FromItem::Join { left, right, .. } => {
            collect_from_item_column_names(left, catalog, env, names);
            collect_from_item_column_names(right, catalog, env, names);
        }
        FromItem::Lateral(source) => collect_from_item_column_names(source, catalog, env, names),
        _ => {}
    }
}

fn collect_select_column_refs(stmt: &SelectStatement, refs: &mut Vec<String>) {
    for target in &stmt.targets {
        collect_expr_column_refs(&target.expr, refs);
    }
    if let Some(expr) = &stmt.where_clause {
        collect_expr_column_refs(expr, refs);
    }
    for item in &stmt.group_by {
        collect_group_by_item_column_refs(item, refs);
    }
    if let Some(expr) = &stmt.having {
        collect_expr_column_refs(expr, refs);
    }
    for item in &stmt.order_by {
        collect_expr_column_refs(&item.expr, refs);
    }
}

fn collect_group_by_item_column_refs(item: &GroupByItem, refs: &mut Vec<String>) {
    match item {
        GroupByItem::Expr(expr) => collect_expr_column_refs(expr, refs),
        GroupByItem::List(exprs) => {
            for expr in exprs {
                collect_expr_column_refs(expr, refs);
            }
        }
        GroupByItem::Empty => {}
        GroupByItem::Rollup(items) | GroupByItem::Cube(items) | GroupByItem::Sets(items) => {
            for item in items {
                collect_group_by_item_column_refs(item, refs);
            }
        }
    }
}

fn collect_expr_column_refs(expr: &SqlExpr, refs: &mut Vec<String>) {
    match expr {
        SqlExpr::Column(name) if !name.contains('.') && !is_internal_plpgsql_name(name) => {
            refs.push(name.clone());
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
        | SqlExpr::Overlaps(left, right)
        | SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
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
            collect_expr_column_refs(left, refs);
            collect_expr_column_refs(right, refs);
        }
        SqlExpr::BinaryOperator { left, right, .. } => {
            collect_expr_column_refs(left, refs);
            collect_expr_column_refs(right, refs);
        }
        SqlExpr::UnaryPlus(expr)
        | SqlExpr::Negate(expr)
        | SqlExpr::BitNot(expr)
        | SqlExpr::PrefixOperator { expr, .. }
        | SqlExpr::Cast(expr, _)
        | SqlExpr::Not(expr)
        | SqlExpr::IsNull(expr)
        | SqlExpr::IsNotNull(expr)
        | SqlExpr::FieldSelect { expr, .. } => collect_expr_column_refs(expr, refs),
        SqlExpr::Subscript { expr, .. } => collect_expr_column_refs(expr, refs),
        SqlExpr::GeometryUnaryOp { expr, .. } => collect_expr_column_refs(expr, refs),
        SqlExpr::GeometryBinaryOp { left, right, .. } => {
            collect_expr_column_refs(left, refs);
            collect_expr_column_refs(right, refs);
        }
        SqlExpr::Collate { expr, .. } => collect_expr_column_refs(expr, refs),
        SqlExpr::AtTimeZone { expr, zone } => {
            collect_expr_column_refs(expr, refs);
            collect_expr_column_refs(zone, refs);
        }
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_expr_column_refs(expr, refs);
            collect_expr_column_refs(pattern, refs);
            if let Some(escape) = escape {
                collect_expr_column_refs(escape, refs);
            }
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            if let Some(arg) = arg {
                collect_expr_column_refs(arg, refs);
            }
            for arm in args {
                collect_expr_column_refs(&arm.expr, refs);
                collect_expr_column_refs(&arm.result, refs);
            }
            if let Some(defresult) = defresult {
                collect_expr_column_refs(defresult, refs);
            }
        }
        SqlExpr::ArrayLiteral(items) | SqlExpr::Row(items) => {
            for item in items {
                collect_expr_column_refs(item, refs);
            }
        }
        SqlExpr::QuantifiedArray { left, array, .. } => {
            collect_expr_column_refs(left, refs);
            collect_expr_column_refs(array, refs);
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            collect_expr_column_refs(array, refs);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_expr_column_refs(lower, refs);
                }
                if let Some(upper) = &subscript.upper {
                    collect_expr_column_refs(upper, refs);
                }
            }
        }
        SqlExpr::Xml(xml) => {
            for child in xml.child_exprs() {
                collect_expr_column_refs(child, refs);
            }
        }
        SqlExpr::JsonQueryFunction(func) => {
            for child in func.child_exprs() {
                collect_expr_column_refs(child, refs);
            }
        }
        SqlExpr::FuncCall {
            args,
            order_by,
            within_group,
            filter,
            over,
            ..
        } => {
            for arg in args.args() {
                collect_expr_column_refs(&arg.value, refs);
            }
            for item in order_by {
                collect_expr_column_refs(&item.expr, refs);
            }
            if let Some(within_group) = within_group {
                for item in within_group {
                    collect_expr_column_refs(&item.expr, refs);
                }
            }
            if let Some(filter) = filter {
                collect_expr_column_refs(filter, refs);
            }
            if let Some(over) = over {
                collect_window_column_refs(over, refs);
            }
        }
        _ => {}
    }
}

fn collect_window_column_refs(spec: &RawWindowSpec, refs: &mut Vec<String>) {
    for expr in &spec.partition_by {
        collect_expr_column_refs(expr, refs);
    }
    for item in &spec.order_by {
        collect_expr_column_refs(&item.expr, refs);
    }
    if let Some(frame) = &spec.frame {
        collect_window_frame_bound_refs(&frame.start_bound, refs);
        collect_window_frame_bound_refs(&frame.end_bound, refs);
    }
}

fn collect_window_frame_bound_refs(bound: &RawWindowFrameBound, refs: &mut Vec<String>) {
    match bound {
        RawWindowFrameBound::OffsetPreceding(expr) | RawWindowFrameBound::OffsetFollowing(expr) => {
            collect_expr_column_refs(expr, refs)
        }
        RawWindowFrameBound::UnboundedPreceding
        | RawWindowFrameBound::CurrentRow
        | RawWindowFrameBound::UnboundedFollowing => {}
    }
}

fn compile_static_query_source(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
    expected: &'static str,
) -> Result<PlannedStmt, ParseError> {
    let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
    match parse_statement(&rewritten_sql)? {
        Statement::Select(stmt) => plan_select_for_env(&stmt, catalog, env),
        Statement::Values(stmt) => plan_values_for_env(&stmt, catalog, env),
        other => Err(ParseError::UnexpectedToken {
            expected,
            actual: format!("{other:?}"),
        }),
    }
}

fn static_query_source_known_columns(sql: &str) -> Option<Vec<QueryColumn>> {
    let normalized = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    if !matches!(
        normalized.as_str(),
        "select * from pg_get_catalog_foreign_keys()"
            | "select * from pg_catalog.pg_get_catalog_foreign_keys()"
    ) {
        return None;
    }

    Some(vec![
        plpgsql_query_column("fktable", SqlType::new(SqlTypeKind::Text)),
        plpgsql_query_column("fkcols", SqlType::array_of(SqlType::new(SqlTypeKind::Text))),
        plpgsql_query_column("pktable", SqlType::new(SqlTypeKind::Text)),
        plpgsql_query_column("pkcols", SqlType::array_of(SqlType::new(SqlTypeKind::Text))),
        plpgsql_query_column("is_array", SqlType::new(SqlTypeKind::Bool)),
        plpgsql_query_column("is_opt", SqlType::new(SqlTypeKind::Bool)),
    ])
}

fn plpgsql_query_column(name: &str, sql_type: SqlType) -> QueryColumn {
    QueryColumn {
        name: name.into(),
        sql_type,
        wire_type_oid: None,
    }
}

fn should_fallback_to_runtime_sql(err: &ParseError) -> bool {
    !matches!(
        err.unpositioned(),
        ParseError::AmbiguousColumn(_)
            | ParseError::DetailedError {
                sqlstate: "42702",
                ..
            }
    )
}

fn compile_return_query_stmt(
    source: &ForQuerySource,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<CompiledStmt, ParseError> {
    let Some(contract) = return_contract else {
        return Err(ParseError::FeatureNotSupported(
            "RETURN QUERY is only supported inside CREATE FUNCTION".into(),
        ));
    };
    let is_setof = match contract {
        FunctionReturnContract::Scalar { setof, .. }
        | FunctionReturnContract::FixedRow { setof, .. }
        | FunctionReturnContract::AnonymousRecord { setof } => *setof,
        FunctionReturnContract::Trigger { .. } | FunctionReturnContract::EventTrigger { .. } => {
            false
        }
    };
    if !is_setof {
        return Err(ParseError::FeatureNotSupported(
            "RETURN QUERY requires a set-returning function".into(),
        ));
    }

    let source = match source {
        ForQuerySource::Static(sql) => compile_return_query_static_source(sql, catalog, env)?,
        ForQuerySource::Execute {
            sql_expr,
            using_exprs,
        } => CompiledForQuerySource::Dynamic {
            sql_expr: compile_expr_text(sql_expr, catalog, env)?,
            using_exprs: using_exprs
                .iter()
                .map(|expr| compile_expr_text(expr, catalog, env))
                .collect::<Result<Vec<_>, _>>()?,
        },
        ForQuerySource::Cursor { .. } => {
            return Err(ParseError::UnexpectedToken {
                expected: "RETURN QUERY SELECT ..., VALUES (...), or EXECUTE ...",
                actual: "cursor query source".into(),
            });
        }
    };
    Ok(CompiledStmt::ReturnQuery { source })
}

fn compile_return_query_static_source(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledForQuerySource, ParseError> {
    let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
    match parse_statement(&rewritten_sql)? {
        Statement::Select(stmt) => Ok(CompiledForQuerySource::Static {
            plan: plan_select_for_env(&stmt, catalog, env)?,
        }),
        Statement::Values(stmt) => Ok(CompiledForQuerySource::Static {
            plan: plan_values_for_env(&stmt, catalog, env)?,
        }),
        Statement::CreateTableAs(_) => Ok(CompiledForQuerySource::NoTuples {
            sql: normalize_sql_context_text(&rewritten_sql),
        }),
        Statement::Unsupported(unsupported)
            if unsupported.feature == "SELECT form"
                && find_next_top_level_keyword(&unsupported.sql, &["into"]).is_some() =>
        {
            Ok(CompiledForQuerySource::NoTuples {
                sql: normalize_sql_context_text(&unsupported.sql),
            })
        }
        other => Err(ParseError::UnexpectedToken {
            expected: "RETURN QUERY SELECT ... or RETURN QUERY VALUES (...)",
            actual: format!("{other:?}"),
        }),
    }
}

fn normalize_sql_context_text(sql: &str) -> String {
    sql.trim().trim_end_matches(';').trim_end().to_string()
}

fn normalize_nonstandard_string_literals(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;

    while i < bytes.len() {
        if bytes[i] == b'\'' {
            let previous = sql[..i].chars().rev().find(|ch| !ch.is_ascii_whitespace());
            if !matches!(previous, Some('E' | 'e' | '&')) {
                out.push('E');
            }
            out.push('\'');
            i += 1;
            while i < bytes.len() {
                out.push(bytes[i] as char);
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 1;
                    out.push(bytes[i] as char);
                } else if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 1;
                        out.push('\'');
                    } else {
                        i += 1;
                        break;
                    }
                }
                i += 1;
            }
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }

    out
}

fn decode_nonstandard_backslash_escapes(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let Some(escaped) = chars.next() else {
            out.push('\\');
            break;
        };
        match escaped {
            '\\' => out.push('\\'),
            '\'' => out.push('\''),
            '0'..='7' => {
                let mut digits = String::from(escaped);
                while digits.len() < 3 {
                    match chars.peek().copied() {
                        Some(next @ '0'..='7') => {
                            digits.push(next);
                            chars.next();
                        }
                        _ => break,
                    }
                }
                if let Ok(code) = u32::from_str_radix(&digits, 8)
                    && let Some(decoded) = char::from_u32(code)
                {
                    out.push(decoded);
                }
            }
            other => {
                out.push('\\');
                out.push(other);
            }
        }
    }
    out
}

fn compile_perform_stmt(
    sql: &str,
    line: usize,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
    let planned = plan_select_for_env(
        &crate::backend::parser::parse_select(&format!("select {rewritten_sql}"))?,
        catalog,
        env,
    )?;
    Ok(CompiledStmt::Perform {
        plan: planned,
        line,
        sql: Some(format!("SELECT {}", sql.trim())),
    })
}

fn compile_dynamic_execute_stmt(
    sql_expr: &str,
    strict: bool,
    into_targets: &[AssignTarget],
    using_exprs: &[String],
    line: usize,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    let mut targets = into_targets
        .iter()
        .map(|target| compile_select_into_target(target, env))
        .collect::<Result<Vec<_>, _>>()?;
    if let [target] = targets.as_mut_slice()
        && target.ty.kind == SqlTypeKind::Record
        && let Some(result_columns) =
            dynamic_sql_literal_result_columns(sql_expr, using_exprs, catalog, env)
    {
        let descriptor = assign_anonymous_record_descriptor(
            result_columns
                .iter()
                .map(|column| (column.name.clone(), column.sql_type))
                .collect(),
        );
        let ty = descriptor.sql_type();
        env.update_slot_type(target.slot, ty);
        target.ty = ty;
    }
    Ok(CompiledStmt::DynamicExecute {
        sql_expr: compile_expr_text(sql_expr, catalog, env)?,
        strict,
        into_targets: targets,
        using_exprs: using_exprs
            .iter()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .collect::<Result<_, _>>()?,
        line,
    })
}

fn compile_cursor_open_source(
    name: &str,
    source: &OpenCursorSource,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<(CompiledCursorOpenSource, bool, Option<PlannedStmt>), ParseError> {
    match source {
        OpenCursorSource::Static(sql) => {
            let plan = compile_static_query_source(sql, catalog, env, "cursor query")?;
            Ok((
                CompiledCursorOpenSource::Static { plan: plan.clone() },
                true,
                Some(plan),
            ))
        }
        OpenCursorSource::Dynamic {
            sql_expr,
            using_exprs,
        } => Ok((
            CompiledCursorOpenSource::Dynamic {
                sql_expr: compile_expr_text(sql_expr, catalog, env)?,
                using_exprs: using_exprs
                    .iter()
                    .map(|expr| compile_expr_text(expr, catalog, env))
                    .collect::<Result<Vec<_>, _>>()?,
            },
            true,
            None,
        )),
        OpenCursorSource::Declared { args } => {
            let cursor =
                env.declared_cursor(name)
                    .cloned()
                    .ok_or_else(|| ParseError::UnexpectedToken {
                        expected: "declared cursor query or OPEN cursor FOR query",
                        actual: name.to_string(),
                    })?;
            let (args, arg_context) =
                compile_declared_cursor_args(name, args, &cursor.params, catalog, env)?;
            let shape_plan = plan_declared_cursor_query_for_shape(&cursor, catalog, env).ok();
            Ok((
                CompiledCursorOpenSource::Declared {
                    query: cursor.query,
                    params: cursor.params,
                    args,
                    arg_context,
                },
                cursor.scrollable,
                shape_plan,
            ))
        }
    }
}

fn compile_declared_cursor_args(
    cursor_name: &str,
    args: &[CursorArg],
    params: &[DeclaredCursorParam],
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<(Vec<CompiledExpr>, Option<String>), ParseError> {
    let mut assigned = vec![None::<String>; params.len()];
    for (arg_index, arg) in args.iter().enumerate() {
        match arg {
            CursorArg::Positional(expr) => {
                let Some(param) = params.get(arg_index) else {
                    return Err(ParseError::UnexpectedToken {
                        expected: "cursor argument",
                        actual: format!("too many arguments for cursor \"{cursor_name}\""),
                    });
                };
                if assigned[arg_index].is_some() {
                    return Err(duplicate_cursor_param_error(cursor_name, &param.name));
                }
                assigned[arg_index] = Some(expr.clone());
            }
            CursorArg::Named { name, expr } => {
                let Some(index) = params
                    .iter()
                    .position(|param| param.name.eq_ignore_ascii_case(name))
                else {
                    return Err(ParseError::UnexpectedToken {
                        expected: "cursor argument name",
                        actual: format!(
                            "cursor \"{cursor_name}\" has no argument named \"{name}\""
                        ),
                    });
                };
                if assigned[index].is_some() {
                    return Err(duplicate_cursor_param_error(
                        cursor_name,
                        &params[index].name,
                    ));
                }
                assigned[index] = Some(expr.clone());
            }
        }
    }
    if let Some(param) = params
        .iter()
        .zip(&assigned)
        .find_map(|(param, expr)| expr.is_none().then_some(param))
    {
        return Err(ParseError::UnexpectedToken {
            expected: "cursor argument",
            actual: format!(
                "not enough arguments for cursor \"{cursor_name}\"; missing \"{}\"",
                param.name
            ),
        });
    }
    let arg_context = declared_cursor_args_context(&assigned, params);
    let args = assigned
        .into_iter()
        .map(|expr| compile_expr_text(&expr.expect("checked above"), catalog, env))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((args, arg_context))
}

fn declared_cursor_args_context(
    assigned: &[Option<String>],
    params: &[DeclaredCursorParam],
) -> Option<String> {
    if assigned.is_empty() {
        return None;
    }
    Some(
        assigned
            .iter()
            .zip(params)
            .map(|(expr, param)| {
                format!(
                    "{} AS {}",
                    expr.as_deref().expect("cursor args checked").trim(),
                    param.name
                )
            })
            .collect::<Vec<_>>()
            .join(", "),
    )
}

fn duplicate_cursor_param_error(cursor_name: &str, param_name: &str) -> ParseError {
    ParseError::UnexpectedToken {
        expected: "cursor argument",
        actual: format!(
            "value for parameter \"{param_name}\" of cursor \"{cursor_name}\" specified more than once"
        ),
    }
}

fn plan_declared_cursor_query_for_shape(
    cursor: &DeclaredCursor,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<PlannedStmt, ParseError> {
    let sql = rewrite_declared_cursor_params_for_plan(&cursor.query, &cursor.params)?;
    compile_static_query_source(&sql, catalog, env, "cursor query")
}

fn compile_open_cursor_stmt(
    name: &str,
    source: &OpenCursorSource,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    let var = env
        .get_var(name)
        .ok_or_else(|| ParseError::UnknownColumn(name.to_string()))?;
    let slot = var.slot;
    let constant = var.constant;
    let (source, scrollable, shape_plan) = compile_cursor_open_source(name, source, catalog, env)?;
    if let Some(plan) = shape_plan {
        env.open_cursor_shapes.insert(slot, plan.columns());
    } else {
        env.open_cursor_shapes.remove(&slot);
    }
    Ok(CompiledStmt::OpenCursor {
        slot,
        name: name.to_string(),
        source,
        scrollable,
        constant,
    })
}

fn compile_exec_sql_stmt(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    if let Some(name) = persistent_object_transition_table_reference(sql, &env.local_ctes) {
        return Err(ParseError::DetailedError {
            message: format!(
                "transition table \"{name}\" cannot be referenced in a persistent object"
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }

    if let Some((target_name, select_sql)) =
        split_cte_prefixed_select_into_target(sql).or_else(|| split_select_into_target(sql))
    {
        let targets = parse_select_into_assign_targets(&target_name)?;
        return match compile_select_into_stmt(&select_sql, &targets, false, catalog, env) {
            Ok(stmt) => Ok(stmt),
            Err(err) if should_fallback_to_runtime_sql(&err) => {
                compile_runtime_select_into_stmt(&select_sql, &targets, false, env)
            }
            Err(err) => Err(err),
        };
    }

    if let Some((target_names, select_sql, strict)) = split_select_with_into_targets(sql) {
        let targets = target_names
            .iter()
            .map(|target| parse_select_into_assign_target(target))
            .collect::<Result<Vec<_>, _>>()?;
        return match compile_select_into_stmt(&select_sql, &targets, strict, catalog, env) {
            Ok(stmt) => Ok(stmt),
            Err(err) if should_fallback_to_runtime_sql(&err) => {
                compile_runtime_select_into_stmt(&select_sql, &targets, strict, env)
            }
            Err(err) => Err(err),
        };
    }
    if let Some((exec_sql, target_names)) = split_dml_returning_into_targets(sql) {
        let targets = target_names
            .iter()
            .map(|target| parse_select_into_assign_target(target))
            .collect::<Result<Vec<_>, _>>()?;
        return compile_exec_returning_into_stmt(&exec_sql, &targets, catalog, env);
    }

    if is_unsupported_plpgsql_transaction_command(sql) {
        return Ok(CompiledStmt::UnsupportedTransactionCommand {
            command: transaction_command_name(sql).to_string(),
        });
    }

    let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
    let stmt = normalize_plpgsql_sql_statement(parse_statement(&rewritten_sql)?, env);
    let outer_scope = outer_scope_for_sql(env);
    let outer_scopes = [outer_scope];
    match stmt {
        Statement::Select(stmt) => Ok(CompiledStmt::Perform {
            plan: plan_select_for_env(&stmt, catalog, env)?,
            line: 1,
            sql: Some(rewritten_sql.clone()),
        }),
        Statement::Values(stmt) => Ok(CompiledStmt::Perform {
            plan: plan_values_for_env(&stmt, catalog, env)?,
            line: 1,
            sql: Some(rewritten_sql.clone()),
        }),
        Statement::Insert(stmt) => match bind_insert_with_outer_scopes(
            &normalize_plpgsql_insert(stmt, env),
            catalog,
            &outer_scopes,
        ) {
            Ok(stmt) => Ok(CompiledStmt::ExecInsert { stmt }),
            Err(err) if should_defer_plpgsql_sql_to_runtime(&err) => Ok(CompiledStmt::RuntimeSql {
                sql: rewritten_sql,
                scope: runtime_sql_scope(env),
            }),
            Err(err) => Err(err),
        },
        Statement::Update(stmt) => match bind_update_with_outer_scopes(
            &normalize_plpgsql_update(stmt, env),
            catalog,
            &outer_scopes,
        ) {
            Ok(stmt) => Ok(CompiledStmt::ExecUpdate { stmt }),
            Err(err) if should_defer_plpgsql_sql_to_runtime(&err) => Ok(CompiledStmt::RuntimeSql {
                sql: rewritten_sql,
                scope: runtime_sql_scope(env),
            }),
            Err(err) => Err(err),
        },
        Statement::Delete(stmt) => match bind_delete_with_outer_scopes(
            &normalize_plpgsql_delete(stmt, env),
            catalog,
            &outer_scopes,
        ) {
            Ok(stmt) => Ok(CompiledStmt::ExecDelete { stmt }),
            Err(err) if should_defer_plpgsql_sql_to_runtime(&err) => Ok(CompiledStmt::RuntimeSql {
                sql: rewritten_sql,
                scope: runtime_sql_scope(env),
            }),
            Err(err) => Err(err),
        },
        Statement::Merge(_) => Ok(CompiledStmt::RuntimeSql {
            sql: rewritten_sql,
            scope: runtime_sql_scope(env),
        }),
        Statement::CreateTable(stmt) if stmt.persistence == TablePersistence::Temporary => {
            Ok(CompiledStmt::ExecSql { sql: rewritten_sql })
        }
        Statement::CreateTable(stmt) => Ok(CompiledStmt::CreateTable { stmt }),
        Statement::CreateTableAs(stmt) => Ok(CompiledStmt::CreateTableAs { stmt }),
        Statement::Analyze(_) => Ok(CompiledStmt::ExecSql { sql: rewritten_sql }),
        Statement::CreateView(_) | Statement::DropTable(_) => Ok(CompiledStmt::RuntimeSql {
            sql: rewritten_sql,
            scope: runtime_sql_scope(env),
        }),
        Statement::Set(stmt) if stmt.name.eq_ignore_ascii_case("jit") => {
            // :HACK: pgrust has no JIT subsystem; PL/pgSQL regression helpers
            // use SET LOCAL jit=0 only to stabilize EXPLAIN.
            Ok(CompiledStmt::Null)
        }
        Statement::Set(stmt) => Ok(CompiledStmt::SetGuc {
            name: stmt.name,
            value: stmt.value,
            is_local: stmt.is_local,
        }),
        Statement::CommentOnFunction(stmt) => Ok(CompiledStmt::CommentOnFunction { stmt }),
        other => Err(ParseError::UnexpectedToken {
            expected: "PL/pgSQL SQL statement",
            actual: format!("{other:?}"),
        }),
    }
}

fn should_defer_plpgsql_sql_to_runtime(err: &ParseError) -> bool {
    should_fallback_to_runtime_sql(err)
        && matches!(
            err.unpositioned(),
            ParseError::UnknownTable(_)
                | ParseError::TableDoesNotExist(_)
                | ParseError::MissingFromClauseEntry(_)
        )
}

fn persistent_object_transition_table_reference(
    sql: &str,
    local_ctes: &[BoundCte],
) -> Option<String> {
    if local_ctes.is_empty() {
        return None;
    }
    let lower = sql.trim_start().to_ascii_lowercase();
    let is_persistent_create = lower.starts_with("create view ")
        || lower.starts_with("create materialized view ")
        || (lower.starts_with("create table ")
            && !lower.starts_with("create table pg_temp.")
            && !lower.starts_with("create table temp ")
            && !lower.starts_with("create table temporary "))
        || (lower.starts_with("create unlogged table ")
            && !lower.starts_with("create unlogged table pg_temp."));
    if !is_persistent_create {
        return None;
    }
    local_ctes
        .iter()
        .find(|cte| sql_references_relation_name(&lower, &cte.name))
        .map(|cte| cte.name.clone())
}

fn sql_references_relation_name(lower_sql: &str, name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    [
        format!(" from {name}"),
        format!(" join {name}"),
        format!(" update {name}"),
        format!(" into {name}"),
        format!(" from \"{name}\""),
        format!(" join \"{name}\""),
        format!(" update \"{name}\""),
        format!(" into \"{name}\""),
    ]
    .iter()
    .any(|needle| lower_sql.contains(needle))
}

fn runtime_sql_scope(env: &CompileEnv) -> RuntimeSqlScope {
    RuntimeSqlScope {
        columns: env.slot_columns(),
        relation_scopes: env.relation_slot_scopes(),
    }
}

fn outer_scope_for_sql(env: &CompileEnv) -> BoundScope {
    runtime_sql_bound_scope(&runtime_sql_scope(env))
}

pub(crate) fn runtime_sql_bound_scope(scope: &RuntimeSqlScope) -> BoundScope {
    bound_scope_from_slot_columns(
        scope.columns.clone(),
        scope.relation_scopes.clone(),
        |column| {
            Expr::Var(Var {
                varno: 1,
                varattno: user_attrno(column.slot),
                varlevelsup: 0,
                vartype: column.sql_type,
                collation_oid: None,
            })
        },
    )
}

pub(crate) const PLPGSQL_RUNTIME_PARAM_BASE: usize = 1_000_000_000;

pub(crate) fn runtime_sql_param_id(slot: usize) -> usize {
    PLPGSQL_RUNTIME_PARAM_BASE + slot
}

pub(crate) fn runtime_sql_param_bound_scope(scope: &RuntimeSqlScope) -> BoundScope {
    bound_scope_from_slot_columns(
        scope.columns.clone(),
        scope.relation_scopes.clone(),
        |column| {
            Expr::Param(Param {
                paramkind: ParamKind::External,
                paramid: runtime_sql_param_id(column.slot),
                paramtype: column.sql_type,
            })
        },
    )
}

fn bound_scope_from_slot_columns(
    columns: Vec<SlotScopeColumn>,
    relation_scopes: Vec<(String, Vec<SlotScopeColumn>)>,
    mut slot_expr: impl FnMut(&SlotScopeColumn) -> Expr,
) -> BoundScope {
    let desc = RelationDesc {
        columns: columns
            .iter()
            .map(|column| column_desc(column.name.clone(), column.sql_type, true))
            .chain(relation_scopes.iter().flat_map(|(_, columns)| {
                columns
                    .iter()
                    .map(|column| column_desc(column.name.clone(), column.sql_type, true))
            }))
            .collect(),
    };
    let mut output_exprs = columns.iter().map(&mut slot_expr).collect::<Vec<_>>();
    let mut scope_columns = columns
        .into_iter()
        .map(|column| crate::backend::parser::analyze::ScopeColumn {
            output_name: column.name,
            hidden: column.hidden,
            qualified_only: false,
            relation_names: Vec::new(),
            relation_output_exprs: Vec::new(),
            hidden_invalid_relation_names: Vec::new(),
            hidden_missing_relation_names: Vec::new(),
            source_relation_oid: None,
            source_attno: None,
            source_columns: Vec::new(),
        })
        .collect::<Vec<_>>();
    let mut relations = Vec::new();
    for (relation_name, relation_columns) in relation_scopes {
        let relation_desc = RelationDesc {
            columns: relation_columns
                .iter()
                .map(|column| column_desc(column.name.clone(), column.sql_type, true))
                .collect(),
        };
        let mut relation_scope = scope_for_relation(Some(&relation_name), &relation_desc);
        for scope_column in &mut relation_scope.columns {
            scope_column.qualified_only = true;
        }
        relations.extend(relation_scope.relations);
        for column in relation_columns {
            output_exprs.push(slot_expr(&column));
        }
        scope_columns.extend(relation_scope.columns);
    }
    BoundScope {
        output_exprs,
        desc,
        columns: scope_columns,
        relations,
    }
}

fn split_select_into_target(sql: &str) -> Option<(String, String)> {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("select into ") {
        return None;
    }
    let rest = trimmed[12..].trim_start();
    let (target, rest) = split_leading_select_into_target(rest)?;
    let select_sql = format!("select {}", rest.trim_start());
    Some((target, select_sql))
}

fn split_cte_prefixed_select_into_target(sql: &str) -> Option<(String, String)> {
    let trimmed = sql.trim_start();
    if !keyword_at(trimmed, 0, "with") {
        return None;
    }
    let select_idx = find_next_top_level_keyword(trimmed, &["select"])?;
    let after_select = trimmed[select_idx + "select".len()..].trim_start();
    if !keyword_at(after_select, 0, "into") {
        return None;
    }
    let rest = after_select["into".len()..].trim_start();
    let (target, rest) = split_leading_select_into_target(rest)?;
    let select_sql = format!(
        "{} select {}",
        trimmed[..select_idx].trim_end(),
        rest.trim_start()
    );
    Some((target, select_sql))
}

fn split_leading_select_into_target(rest: &str) -> Option<(String, &str)> {
    let mut chars = rest.char_indices();
    let end = if rest.starts_with('"') {
        let mut escaped = false;
        let mut end = None;
        for (index, ch) in rest.char_indices().skip(1) {
            if ch == '"' {
                if escaped {
                    escaped = false;
                    continue;
                }
                if rest[index + 1..].starts_with('"') {
                    escaped = true;
                    continue;
                }
                end = Some(index + 1);
                break;
            }
        }
        end?
    } else {
        chars
            .find(|(_, ch)| ch.is_whitespace())
            .map(|(index, _)| index)
            .unwrap_or(rest.len())
    };
    let target = rest[..end].trim().trim_matches('"').to_ascii_lowercase();
    Some((target, &rest[end..]))
}

fn split_select_with_into_targets(sql: &str) -> Option<(Vec<String>, String, bool)> {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("select ") || lower.starts_with("select into ") {
        return None;
    }

    let into_idx = find_next_top_level_keyword(trimmed, &["into"])?;
    let select_sql = trimmed[..into_idx].trim_end();
    if select_sql.eq_ignore_ascii_case("select") {
        return None;
    }

    let mut after_into = trimmed[into_idx + "into".len()..].trim_start();
    let strict = if keyword_at(after_into, 0, "strict") {
        after_into = after_into["strict".len()..].trim_start();
        true
    } else {
        false
    };
    let clause_idx = find_next_top_level_keyword(
        after_into,
        &[
            "from",
            "where",
            "group",
            "having",
            "window",
            "union",
            "intersect",
            "except",
            "order",
            "limit",
            "offset",
            "fetch",
            "for",
        ],
    );
    let (targets_sql, suffix) = match clause_idx {
        Some(idx) => (&after_into[..idx], after_into[idx..].trim_start()),
        None => (after_into, ""),
    };
    let targets = split_top_level_csv(targets_sql)?;
    let rewritten = if suffix.is_empty() {
        select_sql.to_string()
    } else {
        format!("{select_sql} {suffix}")
    };
    Some((targets, rewritten, strict))
}

fn split_dml_returning_into_targets(sql: &str) -> Option<(String, Vec<String>)> {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    if !(lower.starts_with("insert ")
        || lower.starts_with("update ")
        || lower.starts_with("delete ")
        || lower.starts_with("merge "))
    {
        return None;
    }

    let returning_idx = find_next_top_level_keyword(trimmed, &["returning"])?;
    let after_returning = trimmed[returning_idx + "returning".len()..].trim_start();
    let into_idx = find_next_top_level_keyword(after_returning, &["into"])?;
    let returning_sql = after_returning[..into_idx].trim_end();
    if returning_sql.is_empty() {
        return None;
    }
    let targets_sql = after_returning[into_idx + "into".len()..].trim();
    let targets = split_top_level_csv(targets_sql)?;
    let rewritten = format!(
        "{} {}",
        trimmed[..returning_idx + "returning".len()].trim_end(),
        returning_sql,
    );
    Some((rewritten, targets))
}

fn is_unsupported_plpgsql_transaction_command(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    keyword_at(trimmed, 0, "savepoint")
        || keyword_at(trimmed, 0, "release")
        || (keyword_at(trimmed, 0, "rollback")
            && find_next_top_level_keyword(trimmed, &["to"]).is_some())
}

fn transaction_command_name(sql: &str) -> &str {
    let trimmed = sql.trim_start();
    trimmed
        .split_whitespace()
        .next()
        .unwrap_or("transaction command")
}

fn positional_parameter_var_name(index: usize) -> String {
    format!("__pgrust_plpgsql_param_{index}")
}

fn plpgsql_label_alias(scope_index: usize, slot: usize, name: &str) -> String {
    let mut alias = format!("__pgrust_plpgsql_label_{scope_index}_{slot}_");
    for ch in name.chars() {
        alias.push(if is_identifier_char(ch) { ch } else { '_' });
    }
    alias
}

fn is_plpgsql_label_alias(name: &str) -> bool {
    name.starts_with("__pgrust_plpgsql_label_")
}

fn plpgsql_var_alias(slot: usize) -> String {
    format!("__pgrust_plpgsql_var_{slot}")
}

fn is_internal_plpgsql_name(name: &str) -> bool {
    name.starts_with("__pgrust_plpgsql_")
}

fn rewrite_plpgsql_sql_text(sql: &str, env: &CompileEnv) -> Result<String, ParseError> {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut idx = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        if in_single {
            out.push(ch);
            idx += 1;
            if ch == '\'' {
                if bytes.get(idx) == Some(&b'\'') {
                    out.push('\'');
                    idx += 1;
                    continue;
                }
                in_single = false;
            }
            continue;
        }
        if in_double {
            out.push(ch);
            idx += 1;
            if ch == '"' {
                if bytes.get(idx) == Some(&b'"') {
                    out.push('"');
                    idx += 1;
                    continue;
                }
                in_double = false;
            }
            continue;
        }
        if let Some(tag) = dollar_quote_tag_at(sql, idx) {
            if let Some(close) = sql[idx + tag.len()..].find(tag) {
                let end = idx + tag.len() + close + tag.len();
                out.push_str(&sql[idx..end]);
                idx = end;
            } else {
                out.push_str(&sql[idx..]);
                break;
            }
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
                        ParseError::UnexpectedToken {
                            expected: "valid positional parameter reference",
                            actual: sql[idx..end].to_string(),
                        }
                    })?;
                    let name = env.positional_parameter_name(index).ok_or_else(|| {
                        ParseError::UnexpectedToken {
                            expected: "existing positional parameter reference",
                            actual: sql[idx..end].to_string(),
                        }
                    })?;
                    out.push_str(name);
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

fn rewrite_declared_cursor_params_for_plan(
    sql: &str,
    params: &[DeclaredCursorParam],
) -> Result<String, ParseError> {
    if params.is_empty() {
        return Ok(sql.to_string());
    }
    rewrite_identifier_refs(sql, |ident| {
        params
            .iter()
            .find(|param| param.name.eq_ignore_ascii_case(ident))
            .map(|param| format!("(null::{})", param.type_name))
    })
}

fn rewrite_identifier_refs<F>(sql: &str, mut replacement: F) -> Result<String, ParseError>
where
    F: FnMut(&str) -> Option<String>,
{
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut idx = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        if in_single {
            out.push(ch);
            idx += 1;
            if ch == '\'' {
                if bytes.get(idx) == Some(&b'\'') {
                    out.push('\'');
                    idx += 1;
                    continue;
                }
                in_single = false;
            }
            continue;
        }
        if in_double {
            out.push(ch);
            idx += 1;
            if ch == '"' {
                if bytes.get(idx) == Some(&b'"') {
                    out.push('"');
                    idx += 1;
                    continue;
                }
                in_double = false;
            }
            continue;
        }
        if let Some(tag) = dollar_quote_tag_at(sql, idx) {
            if let Some(close) = sql[idx + tag.len()..].find(tag) {
                let end = idx + tag.len() + close + tag.len();
                out.push_str(&sql[idx..end]);
                idx = end;
            } else {
                out.push_str(&sql[idx..]);
                break;
            }
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
                if let Some(value) = replacement(ident) {
                    out.push_str(&value);
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

fn find_next_top_level_keyword(sql: &str, keywords: &[&str]) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut idx = 0usize;
    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        if in_single {
            if ch == '\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                    continue;
                }
                in_single = false;
            }
            idx += 1;
            continue;
        }
        if in_double {
            if ch == '"' {
                if bytes.get(idx + 1) == Some(&b'"') {
                    idx += 2;
                    continue;
                }
                in_double = false;
            }
            idx += 1;
            continue;
        }
        if let Some(tag) = dollar_quote_tag_at(sql, idx) {
            if let Some(close) = sql[idx + tag.len()..].find(tag) {
                idx += tag.len() + close + tag.len();
                continue;
            }
            idx += tag.len();
            continue;
        }

        match ch {
            '\'' => {
                in_single = true;
                idx += 1;
                continue;
            }
            '"' => {
                in_double = true;
                idx += 1;
                continue;
            }
            '[' => {
                bracket_depth += 1;
                idx += 1;
                continue;
            }
            ']' => {
                bracket_depth = bracket_depth.saturating_sub(1);
                idx += 1;
                continue;
            }
            '(' => {
                depth += 1;
                idx += 1;
                continue;
            }
            ')' => {
                depth = depth.saturating_sub(1);
                idx += 1;
                continue;
            }
            _ => {}
        }

        if depth == 0
            && bracket_depth == 0
            && keywords.iter().any(|keyword| keyword_at(sql, idx, keyword))
        {
            return Some(idx);
        }
        idx += 1;
    }
    None
}

fn keyword_at(sql: &str, idx: usize, keyword: &str) -> bool {
    let bytes = sql.as_bytes();
    let end = idx.saturating_add(keyword.len());
    if end > bytes.len() || !sql[idx..end].eq_ignore_ascii_case(keyword) {
        return false;
    }
    let before_ok = idx == 0 || !is_identifier_char(bytes[idx - 1] as char);
    let after_ok = end == bytes.len() || !is_identifier_char(bytes[end] as char);
    before_ok && after_ok
}

fn is_identifier_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn is_identifier_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn split_top_level_csv(input: &str) -> Option<Vec<String>> {
    let bytes = input.as_bytes();
    let mut depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut start = 0usize;
    let mut parts = Vec::new();
    let mut idx = 0usize;
    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        if in_single {
            if ch == '\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                    continue;
                }
                in_single = false;
            }
            idx += 1;
            continue;
        }
        if in_double {
            if ch == '"' {
                if bytes.get(idx + 1) == Some(&b'"') {
                    idx += 2;
                    continue;
                }
                in_double = false;
            }
            idx += 1;
            continue;
        }
        if let Some(tag) = dollar_quote_tag_at(input, idx) {
            if let Some(close) = input[idx + tag.len()..].find(tag) {
                idx += tag.len() + close + tag.len();
                continue;
            }
            idx += tag.len();
            continue;
        }

        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            ',' if depth == 0 && bracket_depth == 0 => {
                let part = input[start..idx].trim();
                if part.is_empty() {
                    return None;
                }
                parts.push(part.to_string());
                start = idx + 1;
            }
            _ => {}
        }
        idx += 1;
    }

    let tail = input[start..].trim();
    if tail.is_empty() {
        return None;
    }
    parts.push(tail.to_string());
    Some(parts)
}

fn dollar_quote_tag_at(sql: &str, idx: usize) -> Option<&str> {
    let bytes = sql.as_bytes();
    if bytes.get(idx) != Some(&b'$') {
        return None;
    }
    let mut end = idx + 1;
    while let Some(byte) = bytes.get(end) {
        let ch = *byte as char;
        if ch == '$' {
            return Some(&sql[idx..=end]);
        }
        if !is_identifier_char(ch) {
            return None;
        }
        end += 1;
    }
    None
}

fn parse_select_into_assign_target(target: &str) -> Result<AssignTarget, ParseError> {
    let trimmed = target.trim();
    match parse_expr(trimmed)? {
        SqlExpr::Column(name) => {
            if let Some((relation, field)) = name.rsplit_once('.') {
                Ok(AssignTarget::Field {
                    relation: relation.to_string(),
                    field: field.to_string(),
                })
            } else {
                Ok(AssignTarget::Name(name))
            }
        }
        SqlExpr::FieldSelect { expr, field } => match *expr {
            SqlExpr::Column(relation) => Ok(AssignTarget::Field { relation, field }),
            _ => Err(ParseError::UnexpectedToken {
                expected: "PL/pgSQL SELECT INTO target",
                actual: trimmed.into(),
            }),
        },
        _ => Err(ParseError::UnexpectedToken {
            expected: "PL/pgSQL SELECT INTO target",
            actual: trimmed.into(),
        }),
    }
}

fn parse_select_into_assign_targets(targets_sql: &str) -> Result<Vec<AssignTarget>, ParseError> {
    split_top_level_csv(targets_sql)
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "PL/pgSQL SELECT INTO target [, ...]",
            actual: targets_sql.into(),
        })?
        .iter()
        .map(|target| parse_select_into_assign_target(target))
        .collect()
}

fn compile_select_into_stmt(
    select_sql: &str,
    target_refs: &[AssignTarget],
    strict: bool,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    let rewritten_sql = rewrite_plpgsql_sql_text(select_sql, env)?;
    let planned = plan_select_for_env(
        &crate::backend::parser::parse_select(&rewritten_sql)?,
        catalog,
        env,
    )?;
    let mut targets = target_refs
        .iter()
        .map(|target| compile_select_into_target(target, env))
        .collect::<Result<Vec<_>, _>>()?;
    if let [target] = targets.as_mut_slice()
        && target.ty.kind == SqlTypeKind::Record
    {
        let descriptor = assign_anonymous_record_descriptor(
            planned
                .columns()
                .into_iter()
                .map(|column| (column.name, column.sql_type))
                .collect(),
        );
        let ty = descriptor.sql_type();
        env.update_slot_type(target.slot, ty);
        target.ty = ty;
    }
    Ok(CompiledStmt::SelectInto {
        plan: planned,
        targets,
        strict,
        strict_params: strict_params_for_sql(select_sql, env),
    })
}

fn compile_runtime_select_into_stmt(
    select_sql: &str,
    target_refs: &[AssignTarget],
    strict: bool,
    env: &mut CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    let targets = target_refs
        .iter()
        .map(|target| compile_select_into_target(target, env))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CompiledStmt::RuntimeSelectInto {
        sql: rewrite_plpgsql_sql_text(select_sql, env)?,
        scope: runtime_sql_scope(env),
        targets,
        strict,
        strict_params: strict_params_for_sql(select_sql, env),
    })
}

fn strict_params_for_sql(sql: &str, env: &CompileEnv) -> Vec<CompiledStrictParam> {
    let mut params = env
        .vars
        .iter()
        .filter(|(name, _)| {
            !name.starts_with('$')
                && !is_plpgsql_label_alias(name)
                && identifier_position(sql, name).is_some()
        })
        .map(|(name, var)| {
            (
                identifier_position(sql, name).unwrap_or(usize::MAX),
                CompiledStrictParam {
                    name: name.clone(),
                    slot: var.slot,
                },
            )
        })
        .collect::<Vec<_>>();
    params.sort_by_key(|(position, _)| *position);
    params.into_iter().map(|(_, param)| param).collect()
}

fn identifier_position(sql: &str, ident: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let ident_len = ident.len();
    let mut offset = 0usize;
    while offset + ident_len <= bytes.len() {
        let rest = &sql[offset..];
        let Some(found) = rest.to_ascii_lowercase().find(&ident.to_ascii_lowercase()) else {
            break;
        };
        let start = offset + found;
        let end = start + ident_len;
        let before_ok =
            start == 0 || !is_sql_ident_char(sql.as_bytes()[start.saturating_sub(1)] as char);
        let after_ok = end == sql.len() || !is_sql_ident_char(sql.as_bytes()[end] as char);
        if before_ok && after_ok {
            return Some(start);
        }
        offset = end;
    }
    None
}

fn is_sql_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn compile_for_query_stmt(
    target: &ForTarget,
    source: &ForQuerySource,
    body: &[Stmt],
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<CompiledStmt, ParseError> {
    let mut implicit_env = implicit_query_loop_record_name(target, env).map(|name| {
        let mut loop_env = env.child();
        loop_env.define_var(name, SqlType::record(RECORD_TYPE_OID));
        loop_env
    });

    let (source, static_columns) = match source {
        ForQuerySource::Static(sql) => {
            match compile_static_query_source(
                sql,
                catalog,
                env,
                "FOR ... IN query LOOP supports SELECT or VALUES; use EXECUTE for dynamic SQL",
            ) {
                Ok(plan) => {
                    let columns =
                        static_query_source_known_columns(sql).unwrap_or_else(|| plan.columns());
                    (
                        CompiledForQuerySource::Static { plan: plan.clone() },
                        Some(columns),
                    )
                }
                Err(err) if should_fallback_to_runtime_sql(&err) => (
                    CompiledForQuerySource::Runtime {
                        sql: rewrite_plpgsql_sql_text(sql, env)?,
                        scope: runtime_sql_scope(env),
                    },
                    static_query_source_known_columns(sql),
                ),
                Err(err) => return Err(err),
            }
        }
        ForQuerySource::Execute {
            sql_expr,
            using_exprs,
        } => (
            CompiledForQuerySource::Dynamic {
                sql_expr: compile_expr_text(sql_expr, catalog, env)?,
                using_exprs: using_exprs
                    .iter()
                    .map(|expr| compile_expr_text(expr, catalog, env))
                    .collect::<Result<Vec<_>, _>>()?,
            },
            None,
        ),
        ForQuerySource::Cursor { name, args } => {
            let (slot, _, _, _) = resolve_assign_target(&AssignTarget::Name(name.clone()), env)?;
            let source = OpenCursorSource::Declared { args: args.clone() };
            let (source, scrollable, shape_plan) =
                compile_cursor_open_source(name, &source, catalog, env)?;
            (
                CompiledForQuerySource::Cursor {
                    slot,
                    name: name.clone(),
                    source,
                    scrollable,
                },
                shape_plan.map(|plan| plan.columns()),
            )
        }
    };
    let target_env = implicit_env.as_mut().unwrap_or(env);
    let target = compile_for_query_target(target, target_env, static_columns.as_deref())?;
    let body = compile_stmt_list(body, catalog, target_env, return_contract)?;
    if let Some(loop_env) = implicit_env {
        env.next_slot = env.next_slot.max(loop_env.next_slot);
    }
    Ok(CompiledStmt::ForQuery {
        target,
        source,
        body,
    })
}

fn implicit_query_loop_record_name<'a>(target: &'a ForTarget, env: &CompileEnv) -> Option<&'a str> {
    match target {
        ForTarget::Single(AssignTarget::Name(name)) if env.get_var(name).is_none() => {
            Some(name.as_str())
        }
        _ => None,
    }
}

fn compile_foreach_stmt(
    target: &ForTarget,
    slice: usize,
    array_expr: &str,
    body: &[Stmt],
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<CompiledStmt, ParseError> {
    Ok(CompiledStmt::ForEach {
        target: compile_for_query_target(target, env, None)?,
        slice,
        array_expr: compile_expr_text(array_expr, catalog, env)?,
        body: compile_stmt_list(body, catalog, env, return_contract)?,
    })
}

fn compile_for_query_target(
    target: &ForTarget,
    env: &mut CompileEnv,
    static_columns: Option<&[QueryColumn]>,
) -> Result<CompiledForQueryTarget, ParseError> {
    let target_refs: &[AssignTarget] = match target {
        ForTarget::Single(target) => std::slice::from_ref(target),
        ForTarget::List(targets) => targets,
    };

    let mut targets = target_refs
        .iter()
        .map(|target| compile_select_into_target(target, env))
        .collect::<Result<Vec<_>, _>>()?;

    if targets.len() > 1
        && targets
            .iter()
            .any(|target| matches!(target.ty.kind, SqlTypeKind::Record | SqlTypeKind::Composite))
    {
        return Err(ParseError::UnexpectedToken {
            expected: "scalar loop variables for multi-target query FOR loop",
            actual: format!("{target:?}"),
        });
    }

    if let ([target], Some(columns)) = (targets.as_mut_slice(), static_columns)
        && target.ty.kind == SqlTypeKind::Record
    {
        let descriptor = assign_anonymous_record_descriptor(
            columns
                .iter()
                .map(|column| (column.name.clone(), column.sql_type))
                .collect(),
        );
        let ty = descriptor.sql_type();
        env.update_slot_type(target.slot, ty);
        target.ty = ty;
    }

    Ok(CompiledForQueryTarget { targets })
}

fn apply_cursor_shape_to_fetch_targets(
    targets: &mut [CompiledSelectIntoTarget],
    columns: Option<&[QueryColumn]>,
    env: &mut CompileEnv,
) {
    let ([target], Some(columns)) = (targets, columns) else {
        return;
    };
    if target.ty.kind != SqlTypeKind::Record {
        return;
    }
    let descriptor = assign_anonymous_record_descriptor(
        columns
            .iter()
            .map(|column| (column.name.clone(), column.sql_type))
            .collect(),
    );
    let ty = descriptor.sql_type();
    env.update_slot_type(target.slot, ty);
    target.ty = ty;
}

fn compile_exec_returning_into_stmt(
    sql: &str,
    target_refs: &[AssignTarget],
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    let stmt = parse_statement(sql)?;
    let outer_scope = outer_scope_for_sql(env);
    match stmt {
        Statement::Insert(stmt) => {
            let stmt = normalize_plpgsql_insert(stmt, env);
            let bound = bind_insert_with_outer_scopes(&stmt, catalog, &[outer_scope])?;
            let targets = compile_dml_into_targets(
                target_refs,
                bound
                    .returning
                    .iter()
                    .map(target_entry_query_column)
                    .collect(),
                env,
            )?;
            Ok(CompiledStmt::ExecInsertInto {
                stmt: bound,
                targets,
            })
        }
        Statement::Update(stmt) => {
            let stmt = normalize_plpgsql_update(stmt, env);
            let bound = bind_update_with_outer_scopes(&stmt, catalog, &[outer_scope])?;
            let targets = compile_dml_into_targets(
                target_refs,
                bound
                    .returning
                    .iter()
                    .map(target_entry_query_column)
                    .collect(),
                env,
            )?;
            Ok(CompiledStmt::ExecUpdateInto {
                stmt: bound,
                targets,
            })
        }
        Statement::Delete(stmt) => {
            let stmt = normalize_plpgsql_delete(stmt, env);
            let bound = bind_delete_with_outer_scopes(&stmt, catalog, &[outer_scope])?;
            let targets = compile_dml_into_targets(
                target_refs,
                bound
                    .returning
                    .iter()
                    .map(target_entry_query_column)
                    .collect(),
                env,
            )?;
            Ok(CompiledStmt::ExecDeleteInto {
                stmt: bound,
                targets,
            })
        }
        Statement::Merge(_) => compile_runtime_select_into_stmt(sql, target_refs, false, env),
        other => Err(ParseError::UnexpectedToken {
            expected: "INSERT/UPDATE/DELETE/MERGE ... RETURNING ... INTO",
            actual: format!("{other:?}"),
        }),
    }
}

fn compile_dml_into_targets(
    target_refs: &[AssignTarget],
    result_columns: Vec<QueryColumn>,
    env: &mut CompileEnv,
) -> Result<Vec<CompiledSelectIntoTarget>, ParseError> {
    let mut targets = target_refs
        .iter()
        .map(|target| compile_select_into_target(target, env))
        .collect::<Result<Vec<_>, _>>()?;
    if let [target] = targets.as_mut_slice()
        && target.ty.kind == SqlTypeKind::Record
    {
        let descriptor = assign_anonymous_record_descriptor(
            result_columns
                .iter()
                .map(|column| (column.name.clone(), column.sql_type))
                .collect(),
        );
        let ty = descriptor.sql_type();
        env.update_slot_type(target.slot, ty);
        target.ty = ty;
    }
    Ok(targets)
}

fn dynamic_sql_literal_result_columns(
    sql_expr: &str,
    using_exprs: &[String],
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Option<Vec<QueryColumn>> {
    let sql = dynamic_sql_literal(sql_expr)?;
    let sql = dynamic_shape_sql(&sql, using_exprs);
    let outer_scope = outer_scope_for_sql(env);
    let stmt = parse_statement(&sql).ok()?;
    match stmt {
        Statement::Select(stmt) => pg_plan_query_with_outer_scopes_and_ctes(
            &stmt,
            catalog,
            std::slice::from_ref(&outer_scope),
            &env.local_ctes,
        )
        .ok()
        .map(|plan| plan.columns()),
        Statement::Values(stmt) => pg_plan_values_query_with_outer_scopes_and_ctes(
            &stmt,
            catalog,
            std::slice::from_ref(&outer_scope),
            &env.local_ctes,
        )
        .ok()
        .map(|plan| plan.columns()),
        Statement::Insert(stmt) => {
            bind_insert_with_outer_scopes(&stmt, catalog, std::slice::from_ref(&outer_scope))
                .ok()
                .map(|bound| {
                    bound
                        .returning
                        .iter()
                        .map(target_entry_query_column)
                        .collect()
                })
        }
        Statement::Update(stmt) => {
            bind_update_with_outer_scopes(&stmt, catalog, std::slice::from_ref(&outer_scope))
                .ok()
                .map(|bound| {
                    bound
                        .returning
                        .iter()
                        .map(target_entry_query_column)
                        .collect()
                })
        }
        Statement::Delete(stmt) => {
            bind_delete_with_outer_scopes(&stmt, catalog, std::slice::from_ref(&outer_scope))
                .ok()
                .map(|bound| {
                    bound
                        .returning
                        .iter()
                        .map(target_entry_query_column)
                        .collect()
                })
        }
        _ => None,
    }
}

fn dynamic_sql_literal(sql_expr: &str) -> Option<String> {
    let expr = parse_expr(sql_expr).ok()?;
    match expr {
        SqlExpr::Const(value) => value.as_text().map(str::to_string),
        _ => None,
    }
}

fn dynamic_shape_sql(sql: &str, using_exprs: &[String]) -> String {
    if using_exprs.is_empty() {
        return sql.trim().trim_end_matches(';').trim_end().to_string();
    }
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut idx = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        if in_single {
            out.push(ch);
            if ch == '\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    out.push('\'');
                    idx += 2;
                    continue;
                }
                in_single = false;
            }
            idx += 1;
            continue;
        }
        if in_double {
            out.push(ch);
            if ch == '"' {
                if bytes.get(idx + 1) == Some(&b'"') {
                    out.push('"');
                    idx += 2;
                    continue;
                }
                in_double = false;
            }
            idx += 1;
            continue;
        }
        if ch == '\'' {
            in_single = true;
            out.push(ch);
            idx += 1;
            continue;
        }
        if ch == '"' {
            in_double = true;
            out.push(ch);
            idx += 1;
            continue;
        }
        if ch == '$' {
            let start = idx + 1;
            let mut end = start;
            while end < bytes.len() && (bytes[end] as char).is_ascii_digit() {
                end += 1;
            }
            if end > start
                && let Ok(param_index) = sql[start..end].parse::<usize>()
                && let Some(expr) = using_exprs.get(param_index - 1)
            {
                out.push('(');
                out.push_str(expr);
                out.push(')');
                idx = end;
                continue;
            }
        }
        out.push(ch);
        idx += 1;
    }
    out.trim().trim_end_matches(';').trim_end().to_string()
}

fn target_entry_query_column(target: &TargetEntry) -> QueryColumn {
    QueryColumn {
        name: target.name.clone(),
        sql_type: target.sql_type,
        wire_type_oid: None,
    }
}

fn compile_stmt_list(
    statements: &[Stmt],
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<Vec<CompiledStmt>, ParseError> {
    statements
        .iter()
        .map(|stmt| compile_stmt(stmt, catalog, env, return_contract))
        .collect()
}

fn compile_expr_text(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledExpr, ParseError> {
    let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
    compile_expr_sql(&rewritten_sql, sql.trim(), catalog, env)
}

fn compile_assignment_expr_text(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledExpr, ParseError> {
    let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
    let rewritten_sql =
        rewrite_plpgsql_assignment_query_expr(&rewritten_sql).unwrap_or(rewritten_sql);
    compile_expr_sql(&rewritten_sql, sql.trim(), catalog, env)
}

fn compile_expr_sql(
    sql: &str,
    source: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledExpr, ParseError> {
    let normalized_sql;
    let sql = if env.nonstandard_string_literals {
        normalized_sql = normalize_nonstandard_string_literals(sql);
        normalized_sql.as_str()
    } else {
        sql
    };
    let parsed = normalize_plpgsql_expr(parse_expr(sql)?, env);
    let (expr, sql_type) = match bind_scalar_expr_in_named_slot_scope(
        &parsed,
        &env.relation_slot_scopes(),
        &env.slot_columns(),
        catalog,
        &env.local_ctes,
    ) {
        Ok(bound) => bound,
        Err(err) => {
            if let Some(expr) = bind_dynamic_record_field_expr(&parsed, env) {
                (expr, SqlType::new(SqlTypeKind::Text))
            } else {
                return Err(err);
            }
        }
    };
    let _ = sql_type;
    let mut subplans = Vec::new();
    let expr = finalize_expr_subqueries(expr, catalog, &mut subplans);
    Ok(CompiledExpr::Scalar {
        expr,
        subplans,
        source: source.trim().to_string(),
    })
}

fn bind_dynamic_record_field_expr(expr: &SqlExpr, env: &CompileEnv) -> Option<Expr> {
    let (name, field) = match expr {
        SqlExpr::FieldSelect { expr, field } => {
            let SqlExpr::Column(name) = expr.as_ref() else {
                return None;
            };
            (name.as_str(), field.as_str())
        }
        SqlExpr::Column(name) => {
            let (name, field) = name.rsplit_once('.')?;
            (name, field)
        }
        _ => return None,
    };
    let var = env.get_var(name)?;
    if !matches!(var.ty.kind, SqlTypeKind::Record) || var.ty.typmod > 0 {
        return None;
    }
    Some(Expr::FieldSelect {
        expr: Box::new(Expr::Var(Var {
            varno: 1,
            varattno: user_attrno(var.slot),
            varlevelsup: 0,
            vartype: var.ty,
            collation_oid: None,
        })),
        field: field.to_string(),
        field_type: SqlType::new(SqlTypeKind::Text),
    })
}

fn compile_condition_text(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledExpr, ParseError> {
    match compile_expr_text(sql, catalog, env) {
        Ok(expr) => Ok(expr),
        Err(ParseError::UnexpectedToken { actual, .. }) if actual == "aggregate function" => {
            if let Some(condition) = parse_plpgsql_query_condition(sql) {
                let query_sql = format!(
                    "select {} from {}",
                    condition.left_expr, condition.from_clause
                );
                let select = normalize_plpgsql_select(
                    crate::backend::parser::parse_select(&query_sql)?,
                    env,
                );
                let plan = plan_select_for_env(&select, catalog, env)?;
                let rhs = match compile_expr_text(condition.right_expr, catalog, env)? {
                    CompiledExpr::Scalar { expr, subplans, .. } if subplans.is_empty() => expr,
                    CompiledExpr::Scalar { .. } => {
                        return Err(ParseError::FeatureNotSupported(
                            "query-style PL/pgSQL conditions do not support subqueries on the comparison value".into(),
                        ));
                    }
                    CompiledExpr::QueryCompare { .. } => {
                        return Err(ParseError::FeatureNotSupported(
                            "query-style PL/pgSQL conditions do not support query comparisons on both sides".into(),
                        ))
                    }
                };
                return Ok(CompiledExpr::QueryCompare {
                    plan,
                    op: condition.op,
                    rhs,
                    source: sql.trim().to_string(),
                });
            }
            Err(ParseError::UnexpectedToken {
                expected: "non-aggregate expression",
                actual,
            })
        }
        Err(err) => Err(err),
    }
}

fn compile_return_select_expr(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<Option<(String, PlannedStmt)>, ParseError> {
    let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
    let Some(from_idx) = find_keyword_at_top_level(&rewritten_sql, "from") else {
        return Ok(None);
    };
    let expr = rewritten_sql[..from_idx].trim();
    let from_clause = rewritten_sql[from_idx + "from".len()..].trim();
    if expr.is_empty() || from_clause.is_empty() || !looks_like_aggregate_expr(expr) {
        return Ok(None);
    }
    // :HACK: PL/pgSQL normally compiles SQL expressions through SPI.  Keep this
    // focused on PostgreSQL regression's aggregate RETURN shape until PL/pgSQL
    // has a general SPI expression-plan path.
    let query_sql = format!("select {expr} from {from_clause}");
    let select = normalize_plpgsql_select(crate::backend::parser::parse_select(&query_sql)?, env);
    let plan = plan_select_for_env(&select, catalog, env)?;
    Ok(Some((query_sql, plan)))
}

struct ParsedQueryCondition<'a> {
    left_expr: &'a str,
    op: QueryCompareOp,
    right_expr: &'a str,
    from_clause: &'a str,
}

fn parse_plpgsql_query_condition(sql: &str) -> Option<ParsedQueryCondition<'_>> {
    let from_idx = find_keyword_at_top_level(sql, "from")?;
    let before_from = sql[..from_idx].trim();
    let after_from = sql[from_idx + "from".len()..].trim();
    if before_from.is_empty() || after_from.is_empty() {
        return None;
    }

    let (left, op, right) = split_top_level_comparison(before_from)?;
    if !looks_like_aggregate_expr(left) {
        return None;
    }

    Some(ParsedQueryCondition {
        left_expr: left,
        op: query_compare_op(op)?,
        right_expr: right,
        from_clause: after_from,
    })
}

fn rewrite_plpgsql_query_condition(sql: &str) -> Option<String> {
    let parsed = parse_plpgsql_query_condition(sql)?;
    Some(format!(
        "(select {} from {}) {} {}",
        parsed.left_expr,
        parsed.from_clause,
        render_query_compare_op(parsed.op),
        parsed.right_expr
    ))
}

fn rewrite_plpgsql_assignment_query_expr(sql: &str) -> Option<String> {
    let from_idx = find_keyword_at_top_level(sql, "from")?;
    let expr = sql[..from_idx].trim();
    let from_clause = sql[from_idx + "from".len()..].trim();
    if expr.is_empty() || from_clause.is_empty() {
        return None;
    }
    Some(format!("(select {expr} from {from_clause})"))
}

fn query_compare_op(op: &str) -> Option<QueryCompareOp> {
    Some(match op {
        "=" => QueryCompareOp::Eq,
        "<>" | "!=" => QueryCompareOp::NotEq,
        "<" => QueryCompareOp::Lt,
        "<=" => QueryCompareOp::LtEq,
        ">" => QueryCompareOp::Gt,
        ">=" => QueryCompareOp::GtEq,
        "is distinct from" => QueryCompareOp::IsDistinctFrom,
        "is not distinct from" => QueryCompareOp::IsNotDistinctFrom,
        _ => return None,
    })
}

fn render_query_compare_op(op: QueryCompareOp) -> &'static str {
    match op {
        QueryCompareOp::Eq => "=",
        QueryCompareOp::NotEq => "!=",
        QueryCompareOp::Lt => "<",
        QueryCompareOp::LtEq => "<=",
        QueryCompareOp::Gt => ">",
        QueryCompareOp::GtEq => ">=",
        QueryCompareOp::IsDistinctFrom => "is distinct from",
        QueryCompareOp::IsNotDistinctFrom => "is not distinct from",
    }
}

fn split_top_level_comparison(input: &str) -> Option<(&str, &'static str, &str)> {
    const OPERATORS: [&str; 8] = [
        " is not distinct from ",
        " is distinct from ",
        ">=",
        "<=",
        "<>",
        "!=",
        "=",
        ">",
    ];

    for op in OPERATORS {
        if let Some(idx) = find_top_level_token(input, op) {
            let left = input[..idx].trim();
            let right = input[idx + op.len()..].trim();
            if !left.is_empty() && !right.is_empty() {
                return Some((left, op.trim(), right));
            }
        }
    }

    if let Some(idx) = find_top_level_token(input, "<") {
        let left = input[..idx].trim();
        let right = input[idx + 1..].trim();
        if !left.is_empty() && !right.is_empty() {
            return Some((left, "<", right));
        }
    }

    None
}

fn looks_like_aggregate_expr(expr: &str) -> bool {
    let trimmed = expr.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    [
        "count(",
        "sum(",
        "avg(",
        "min(",
        "max(",
        "bool_and(",
        "bool_or(",
        "every(",
        "array_agg(",
        "string_agg(",
        "json_agg(",
        "jsonb_agg(",
        "json_object_agg(",
        "jsonb_object_agg(",
        "xmlagg(",
    ]
    .iter()
    .any(|prefix| lower.starts_with(prefix))
}

fn find_keyword_at_top_level(input: &str, keyword: &str) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let keyword_len = keyword.len();

    for (idx, ch) in input.char_indices() {
        if in_single {
            if ch == '\'' {
                in_single = false;
            }
            continue;
        }
        if in_double {
            if ch == '"' {
                in_double = false;
            }
            continue;
        }

        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ => {}
        }

        if depth != 0 {
            continue;
        }

        let tail = &input[idx..];
        if tail.len() < keyword_len {
            continue;
        }
        if !tail[..keyword_len].eq_ignore_ascii_case(keyword) {
            continue;
        }
        let prev_ok = idx == 0
            || !input[..idx]
                .chars()
                .next_back()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_');
        let next_ok = tail[keyword_len..]
            .chars()
            .next()
            .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'));
        if prev_ok && next_ok {
            return Some(idx);
        }
    }

    None
}

fn find_top_level_token(input: &str, token: &str) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;

    for (idx, ch) in input.char_indices() {
        if in_single {
            if ch == '\'' {
                in_single = false;
            }
            continue;
        }
        if in_double {
            if ch == '"' {
                in_double = false;
            }
            continue;
        }

        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ => {}
        }

        if depth == 0 && input[idx..].starts_with(token) {
            return Some(idx);
        }
    }

    None
}

fn normalize_plpgsql_sql_statement(stmt: Statement, env: &CompileEnv) -> Statement {
    match stmt {
        Statement::Update(mut stmt) => {
            for assignment in &mut stmt.assignments {
                assignment.expr = normalize_plpgsql_expr(assignment.expr.clone(), env);
                normalize_assignment_target_subscripts(&mut assignment.target, env);
            }
            if let Some(where_clause) = stmt.where_clause.take() {
                stmt.where_clause = Some(normalize_plpgsql_expr(where_clause, env));
            }
            for item in &mut stmt.returning {
                item.expr = normalize_plpgsql_expr(item.expr.clone(), env);
            }
            Statement::Update(stmt)
        }
        Statement::Delete(mut stmt) => {
            if let Some(where_clause) = stmt.where_clause.take() {
                stmt.where_clause = Some(normalize_plpgsql_expr(where_clause, env));
            }
            for item in &mut stmt.returning {
                item.expr = normalize_plpgsql_expr(item.expr.clone(), env);
            }
            Statement::Delete(stmt)
        }
        Statement::Insert(mut stmt) => {
            normalize_insert_source(&mut stmt.source, env);
            if let Some(clause) = &mut stmt.on_conflict
                && matches!(clause.action, OnConflictAction::Update)
            {
                for assignment in &mut clause.assignments {
                    assignment.expr = normalize_plpgsql_expr(assignment.expr.clone(), env);
                    normalize_assignment_target_subscripts(&mut assignment.target, env);
                }
                if let Some(predicate) = clause.where_clause.take() {
                    clause.where_clause = Some(normalize_plpgsql_expr(predicate, env));
                }
            }
            for item in &mut stmt.returning {
                item.expr = normalize_plpgsql_expr(item.expr.clone(), env);
            }
            Statement::Insert(stmt)
        }
        other => other,
    }
}

fn normalize_assignment_target_subscripts(target: &mut AssignmentTarget, env: &CompileEnv) {
    for subscript in &mut target.subscripts {
        if let Some(lower) = subscript.lower.take() {
            subscript.lower = Some(Box::new(normalize_plpgsql_expr(*lower, env)));
        }
        if let Some(upper) = subscript.upper.take() {
            subscript.upper = Some(Box::new(normalize_plpgsql_expr(*upper, env)));
        }
    }
    for indirection in &mut target.indirection {
        if let AssignmentTargetIndirection::Subscript(subscript) = indirection {
            if let Some(lower) = subscript.lower.take() {
                subscript.lower = Some(Box::new(normalize_plpgsql_expr(*lower, env)));
            }
            if let Some(upper) = subscript.upper.take() {
                subscript.upper = Some(Box::new(normalize_plpgsql_expr(*upper, env)));
            }
        }
    }
}

fn normalize_insert_source(source: &mut InsertSource, env: &CompileEnv) {
    match source {
        InsertSource::Values(rows) => {
            for row in rows {
                for expr in row {
                    *expr = normalize_plpgsql_expr(expr.clone(), env);
                }
            }
        }
        InsertSource::Select(select) => {
            *select = Box::new(normalize_plpgsql_select((**select).clone(), env));
        }
        InsertSource::DefaultValues => {}
    }
}

fn normalize_plpgsql_expr(expr: SqlExpr, env: &CompileEnv) -> SqlExpr {
    match expr {
        SqlExpr::Column(name) => {
            if let Some(expr) = normalize_labeled_column_name(&name, env) {
                return expr;
            }
            if env.variable_conflict == PlpgsqlVariableConflict::UseVariable
                && !name.contains('.')
                && !is_internal_plpgsql_name(&name)
                && let Some(var) = env.get_var(&name)
            {
                return SqlExpr::Column(plpgsql_var_alias(var.slot));
            }
            if let Some((base, field)) = name.rsplit_once('.')
                && let Some(var) = env.get_var(base)
                && matches!(var.ty.kind, SqlTypeKind::Record | SqlTypeKind::Composite)
            {
                return SqlExpr::FieldSelect {
                    expr: Box::new(SqlExpr::Column(base.to_string())),
                    field: field.to_string(),
                };
            }
            SqlExpr::Column(name)
        }
        SqlExpr::FieldSelect { expr, field } => {
            if let Some(normalized) = normalize_labeled_field_select(&expr, &field, env) {
                return normalized;
            }
            SqlExpr::FieldSelect {
                expr: Box::new(normalize_plpgsql_expr(*expr, env)),
                field,
            }
        }
        SqlExpr::Add(left, right) => SqlExpr::Add(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Sub(left, right) => SqlExpr::Sub(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::BitAnd(left, right) => SqlExpr::BitAnd(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::BitOr(left, right) => SqlExpr::BitOr(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::BitXor(left, right) => SqlExpr::BitXor(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Shl(left, right) => SqlExpr::Shl(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Shr(left, right) => SqlExpr::Shr(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Mul(left, right) => SqlExpr::Mul(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Div(left, right) => SqlExpr::Div(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Mod(left, right) => SqlExpr::Mod(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Concat(left, right) => SqlExpr::Concat(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::BinaryOperator { op, left, right } => SqlExpr::BinaryOperator {
            op,
            left: Box::new(normalize_plpgsql_expr(*left, env)),
            right: Box::new(normalize_plpgsql_expr(*right, env)),
        },
        SqlExpr::UnaryPlus(inner) => {
            SqlExpr::UnaryPlus(Box::new(normalize_plpgsql_expr(*inner, env)))
        }
        SqlExpr::Negate(inner) => SqlExpr::Negate(Box::new(normalize_plpgsql_expr(*inner, env))),
        SqlExpr::BitNot(inner) => SqlExpr::BitNot(Box::new(normalize_plpgsql_expr(*inner, env))),
        SqlExpr::Subscript { expr, index } => SqlExpr::Subscript {
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
            index,
        },
        SqlExpr::GeometryUnaryOp { op, expr } => SqlExpr::GeometryUnaryOp {
            op,
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
        },
        SqlExpr::GeometryBinaryOp { op, left, right } => SqlExpr::GeometryBinaryOp {
            op,
            left: Box::new(normalize_plpgsql_expr(*left, env)),
            right: Box::new(normalize_plpgsql_expr(*right, env)),
        },
        SqlExpr::PrefixOperator { op, expr } => SqlExpr::PrefixOperator {
            op,
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
        },
        SqlExpr::Cast(inner, ty) => {
            SqlExpr::Cast(Box::new(normalize_plpgsql_expr(*inner, env)), ty)
        }
        SqlExpr::Collate { expr, collation } => SqlExpr::Collate {
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
            collation,
        },
        SqlExpr::AtTimeZone { expr, zone } => SqlExpr::AtTimeZone {
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
            zone: Box::new(normalize_plpgsql_expr(*zone, env)),
        },
        SqlExpr::Eq(left, right) => SqlExpr::Eq(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::NotEq(left, right) => SqlExpr::NotEq(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Lt(left, right) => SqlExpr::Lt(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::LtEq(left, right) => SqlExpr::LtEq(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Gt(left, right) => SqlExpr::Gt(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::GtEq(left, right) => SqlExpr::GtEq(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::RegexMatch(left, right) => SqlExpr::RegexMatch(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => SqlExpr::Like {
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
            pattern: Box::new(normalize_plpgsql_expr(*pattern, env)),
            escape: escape.map(|expr| Box::new(normalize_plpgsql_expr(*expr, env))),
            case_insensitive,
            negated,
        },
        SqlExpr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => SqlExpr::Similar {
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
            pattern: Box::new(normalize_plpgsql_expr(*pattern, env)),
            escape: escape.map(|expr| Box::new(normalize_plpgsql_expr(*expr, env))),
            negated,
        },
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => SqlExpr::Case {
            arg: arg.map(|expr| Box::new(normalize_plpgsql_expr(*expr, env))),
            args: args
                .into_iter()
                .map(|arm| normalize_plpgsql_case_when(arm, env))
                .collect(),
            defresult: defresult.map(|expr| Box::new(normalize_plpgsql_expr(*expr, env))),
        },
        SqlExpr::And(left, right) => SqlExpr::And(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Or(left, right) => SqlExpr::Or(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Not(inner) => SqlExpr::Not(Box::new(normalize_plpgsql_expr(*inner, env))),
        SqlExpr::IsNull(inner) => SqlExpr::IsNull(Box::new(normalize_plpgsql_expr(*inner, env))),
        SqlExpr::IsNotNull(inner) => {
            SqlExpr::IsNotNull(Box::new(normalize_plpgsql_expr(*inner, env)))
        }
        SqlExpr::IsDistinctFrom(left, right) => SqlExpr::IsDistinctFrom(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::IsNotDistinctFrom(left, right) => SqlExpr::IsNotDistinctFrom(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::ArrayLiteral(items) => SqlExpr::ArrayLiteral(
            items
                .into_iter()
                .map(|item| normalize_plpgsql_expr(item, env))
                .collect(),
        ),
        SqlExpr::Row(items) => SqlExpr::Row(
            items
                .into_iter()
                .map(|item| normalize_plpgsql_expr(item, env))
                .collect(),
        ),
        SqlExpr::ArrayOverlap(left, right) => SqlExpr::ArrayOverlap(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::ArrayContains(left, right) => SqlExpr::ArrayContains(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::ArrayContained(left, right) => SqlExpr::ArrayContained(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonbContains(left, right) => SqlExpr::JsonbContains(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonbContained(left, right) => SqlExpr::JsonbContained(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonbExists(left, right) => SqlExpr::JsonbExists(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonbExistsAny(left, right) => SqlExpr::JsonbExistsAny(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonbExistsAll(left, right) => SqlExpr::JsonbExistsAll(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonbPathExists(left, right) => SqlExpr::JsonbPathExists(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonbPathMatch(left, right) => SqlExpr::JsonbPathMatch(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::ScalarSubquery(select) => {
            SqlExpr::ScalarSubquery(Box::new(normalize_plpgsql_select(*select, env)))
        }
        SqlExpr::ArraySubquery(select) => {
            SqlExpr::ArraySubquery(Box::new(normalize_plpgsql_select(*select, env)))
        }
        SqlExpr::Exists(select) => {
            SqlExpr::Exists(Box::new(normalize_plpgsql_select(*select, env)))
        }
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => SqlExpr::InSubquery {
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
            subquery: Box::new(normalize_plpgsql_select(*subquery, env)),
            negated,
        },
        SqlExpr::QuantifiedSubquery {
            left,
            op,
            is_all,
            subquery,
        } => SqlExpr::QuantifiedSubquery {
            left: Box::new(normalize_plpgsql_expr(*left, env)),
            op,
            is_all,
            subquery: Box::new(normalize_plpgsql_select(*subquery, env)),
        },
        SqlExpr::QuantifiedArray {
            left,
            op,
            is_all,
            array,
        } => SqlExpr::QuantifiedArray {
            left: Box::new(normalize_plpgsql_expr(*left, env)),
            op,
            is_all,
            array: Box::new(normalize_plpgsql_expr(*array, env)),
        },
        SqlExpr::ArraySubscript { array, subscripts } => SqlExpr::ArraySubscript {
            array: Box::new(normalize_plpgsql_expr(*array, env)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| normalize_plpgsql_array_subscript(subscript, env))
                .collect(),
        },
        SqlExpr::Xml(xml) => SqlExpr::Xml(Box::new(normalize_plpgsql_xml(*xml, env))),
        SqlExpr::JsonGet(left, right) => SqlExpr::JsonGet(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonGetText(left, right) => SqlExpr::JsonGetText(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonPath(left, right) => SqlExpr::JsonPath(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonPathText(left, right) => SqlExpr::JsonPathText(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            within_group,
            distinct,
            func_variadic,
            filter,
            null_treatment,
            over,
        } => SqlExpr::FuncCall {
            name,
            args: normalize_plpgsql_call_args(args, env),
            order_by: order_by
                .into_iter()
                .map(|item| normalize_plpgsql_order_by_item(item, env))
                .collect(),
            within_group: within_group.map(|items| {
                items
                    .into_iter()
                    .map(|item| normalize_plpgsql_order_by_item(item, env))
                    .collect()
            }),
            distinct,
            func_variadic,
            filter: filter.map(|expr| Box::new(normalize_plpgsql_expr(*expr, env))),
            null_treatment,
            over: over.map(|spec| normalize_plpgsql_window_spec(spec, env)),
        },
        other => other,
    }
}

fn normalize_plpgsql_case_when(mut arm: SqlCaseWhen, env: &CompileEnv) -> SqlCaseWhen {
    arm.expr = normalize_plpgsql_expr(arm.expr, env);
    arm.result = normalize_plpgsql_expr(arm.result, env);
    arm
}

fn normalize_plpgsql_call_args(args: SqlCallArgs, env: &CompileEnv) -> SqlCallArgs {
    match args {
        SqlCallArgs::Star => SqlCallArgs::Star,
        SqlCallArgs::Args(args) => SqlCallArgs::Args(
            args.into_iter()
                .map(|mut arg| {
                    arg.value = normalize_plpgsql_expr(arg.value, env);
                    arg
                })
                .collect(),
        ),
    }
}

fn normalize_plpgsql_order_by_item(mut item: OrderByItem, env: &CompileEnv) -> OrderByItem {
    item.expr = normalize_plpgsql_expr(item.expr, env);
    item
}

fn normalize_plpgsql_array_subscript(
    mut subscript: ArraySubscript,
    env: &CompileEnv,
) -> ArraySubscript {
    subscript.lower = subscript
        .lower
        .map(|expr| Box::new(normalize_plpgsql_expr(*expr, env)));
    subscript.upper = subscript
        .upper
        .map(|expr| Box::new(normalize_plpgsql_expr(*expr, env)));
    subscript
}

fn normalize_plpgsql_xml(mut xml: RawXmlExpr, env: &CompileEnv) -> RawXmlExpr {
    xml.named_args = xml
        .named_args
        .into_iter()
        .map(|expr| normalize_plpgsql_expr(expr, env))
        .collect();
    xml.args = xml
        .args
        .into_iter()
        .map(|expr| normalize_plpgsql_expr(expr, env))
        .collect();
    xml
}

fn normalize_plpgsql_window_spec(mut spec: RawWindowSpec, env: &CompileEnv) -> RawWindowSpec {
    spec.partition_by = spec
        .partition_by
        .into_iter()
        .map(|expr| normalize_plpgsql_expr(expr, env))
        .collect();
    spec.order_by = spec
        .order_by
        .into_iter()
        .map(|item| normalize_plpgsql_order_by_item(item, env))
        .collect();
    spec.frame = spec
        .frame
        .map(|frame| Box::new(normalize_plpgsql_window_frame(*frame, env)));
    spec
}

fn normalize_plpgsql_window_frame(mut frame: RawWindowFrame, env: &CompileEnv) -> RawWindowFrame {
    frame.start_bound = normalize_plpgsql_window_frame_bound(frame.start_bound, env);
    frame.end_bound = normalize_plpgsql_window_frame_bound(frame.end_bound, env);
    frame
}

fn normalize_plpgsql_window_frame_bound(
    bound: RawWindowFrameBound,
    env: &CompileEnv,
) -> RawWindowFrameBound {
    match bound {
        RawWindowFrameBound::OffsetPreceding(expr) => {
            RawWindowFrameBound::OffsetPreceding(Box::new(normalize_plpgsql_expr(*expr, env)))
        }
        RawWindowFrameBound::OffsetFollowing(expr) => {
            RawWindowFrameBound::OffsetFollowing(Box::new(normalize_plpgsql_expr(*expr, env)))
        }
        other => other,
    }
}

fn normalize_labeled_column_name(name: &str, env: &CompileEnv) -> Option<SqlExpr> {
    let (label_and_var, field) = name.rsplit_once('.')?;
    let Some((label, qualifier)) = label_and_var.rsplit_once('.') else {
        return env
            .get_labeled_var(label_and_var, field)
            .map(|scope_var| SqlExpr::Column(scope_var.alias.clone()));
    };
    if let Some(scope_var) = env.get_labeled_var(label, qualifier)
        && matches!(
            scope_var.var.ty.kind,
            SqlTypeKind::Record | SqlTypeKind::Composite
        )
    {
        return Some(SqlExpr::FieldSelect {
            expr: Box::new(SqlExpr::Column(scope_var.alias.clone())),
            field: field.to_string(),
        });
    }
    if env
        .get_labeled_relation_field(label, qualifier, field)
        .is_some()
    {
        return Some(SqlExpr::FieldSelect {
            expr: Box::new(SqlExpr::Column(qualifier.to_string())),
            field: field.to_string(),
        });
    }
    None
}

fn normalize_labeled_field_select(
    expr: &SqlExpr,
    field: &str,
    env: &CompileEnv,
) -> Option<SqlExpr> {
    if let SqlExpr::Column(label) = expr
        && let Some(scope_var) = env.get_labeled_var(label, field)
    {
        return Some(SqlExpr::Column(scope_var.alias.clone()));
    }

    if let SqlExpr::Column(label) = expr
        && let Some((qualifier, nested_field)) = field.rsplit_once('.')
    {
        if let Some(scope_var) = env.get_labeled_var(label, qualifier)
            && matches!(
                scope_var.var.ty.kind,
                SqlTypeKind::Record | SqlTypeKind::Composite
            )
        {
            return Some(SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column(scope_var.alias.clone())),
                field: nested_field.to_string(),
            });
        }
        if env
            .get_labeled_relation_field(label, qualifier, nested_field)
            .is_some()
        {
            return Some(SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column(qualifier.to_string())),
                field: nested_field.to_string(),
            });
        }
    }

    let SqlExpr::FieldSelect {
        expr,
        field: qualifier,
    } = expr
    else {
        return None;
    };
    let SqlExpr::Column(label) = expr.as_ref() else {
        return None;
    };
    if let Some((qualifier, nested_field)) = field.rsplit_once('.') {
        if let Some(scope_var) = env.get_labeled_var(label, qualifier)
            && matches!(
                scope_var.var.ty.kind,
                SqlTypeKind::Record | SqlTypeKind::Composite
            )
        {
            return Some(SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column(scope_var.alias.clone())),
                field: nested_field.to_string(),
            });
        }
        if env
            .get_labeled_relation_field(label, qualifier, nested_field)
            .is_some()
        {
            return Some(SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column(qualifier.to_string())),
                field: nested_field.to_string(),
            });
        }
    }
    if let Some(scope_var) = env.get_labeled_var(label, qualifier)
        && matches!(
            scope_var.var.ty.kind,
            SqlTypeKind::Record | SqlTypeKind::Composite
        )
    {
        return Some(SqlExpr::FieldSelect {
            expr: Box::new(SqlExpr::Column(scope_var.alias.clone())),
            field: field.to_string(),
        });
    }
    if env
        .get_labeled_relation_field(label, qualifier, field)
        .is_some()
    {
        return Some(SqlExpr::FieldSelect {
            expr: Box::new(SqlExpr::Column(qualifier.clone())),
            field: field.to_string(),
        });
    }
    None
}

fn normalize_plpgsql_select(mut stmt: SelectStatement, env: &CompileEnv) -> SelectStatement {
    stmt.with = stmt
        .with
        .into_iter()
        .map(|mut cte| {
            cte.body = normalize_plpgsql_cte_body(cte.body, env);
            cte
        })
        .collect();
    stmt.targets = stmt
        .targets
        .into_iter()
        .map(|mut target| {
            target.expr = normalize_plpgsql_expr(target.expr, env);
            target
        })
        .collect();
    stmt.where_clause = stmt
        .where_clause
        .map(|expr| normalize_plpgsql_expr(expr, env));
    stmt.group_by = stmt
        .group_by
        .into_iter()
        .map(|item| normalize_plpgsql_group_by_item(item, env))
        .collect();
    stmt.having = stmt.having.map(|expr| normalize_plpgsql_expr(expr, env));
    stmt.order_by = stmt
        .order_by
        .into_iter()
        .map(|mut item| {
            item.expr = normalize_plpgsql_expr(item.expr, env);
            item
        })
        .collect();
    stmt.from = stmt.from.map(|from| normalize_plpgsql_from_item(from, env));
    if let Some(set_operation) = stmt.set_operation.as_mut() {
        set_operation.inputs = set_operation
            .inputs
            .drain(..)
            .map(|input| normalize_plpgsql_select(input, env))
            .collect();
    }
    stmt
}

fn normalize_plpgsql_group_by_item(item: GroupByItem, env: &CompileEnv) -> GroupByItem {
    match item {
        GroupByItem::Expr(expr) => GroupByItem::Expr(normalize_plpgsql_expr(expr, env)),
        GroupByItem::Empty => GroupByItem::Empty,
        GroupByItem::List(exprs) => GroupByItem::List(
            exprs
                .into_iter()
                .map(|expr| normalize_plpgsql_expr(expr, env))
                .collect(),
        ),
        GroupByItem::Rollup(items) => GroupByItem::Rollup(
            items
                .into_iter()
                .map(|item| normalize_plpgsql_group_by_item(item, env))
                .collect(),
        ),
        GroupByItem::Cube(items) => GroupByItem::Cube(
            items
                .into_iter()
                .map(|item| normalize_plpgsql_group_by_item(item, env))
                .collect(),
        ),
        GroupByItem::Sets(items) => GroupByItem::Sets(
            items
                .into_iter()
                .map(|item| normalize_plpgsql_group_by_item(item, env))
                .collect(),
        ),
    }
}

fn normalize_plpgsql_cte_body(body: CteBody, env: &CompileEnv) -> CteBody {
    match body {
        CteBody::Select(select) => {
            CteBody::Select(Box::new(normalize_plpgsql_select(*select, env)))
        }
        CteBody::Values(values) => CteBody::Values(normalize_plpgsql_values(values, env)),
        CteBody::Insert(insert) => {
            CteBody::Insert(Box::new(normalize_plpgsql_insert(*insert, env)))
        }
        CteBody::Update(update) => {
            CteBody::Update(Box::new(normalize_plpgsql_update(*update, env)))
        }
        CteBody::Delete(delete) => {
            CteBody::Delete(Box::new(normalize_plpgsql_delete(*delete, env)))
        }
        CteBody::Merge(merge) => CteBody::Merge(Box::new(normalize_plpgsql_merge(*merge, env))),
        CteBody::RecursiveUnion {
            all,
            left_nested,
            anchor_with_is_subquery,
            anchor,
            recursive,
        } => CteBody::RecursiveUnion {
            all,
            left_nested,
            anchor_with_is_subquery,
            anchor: Box::new(normalize_plpgsql_cte_body(*anchor, env)),
            recursive: Box::new(normalize_plpgsql_select(*recursive, env)),
        },
    }
}

fn normalize_plpgsql_insert(mut stmt: InsertStatement, env: &CompileEnv) -> InsertStatement {
    stmt.with = stmt
        .with
        .into_iter()
        .map(|mut cte| {
            cte.body = normalize_plpgsql_cte_body(cte.body, env);
            cte
        })
        .collect();
    stmt.columns = stmt.columns.map(|columns| {
        columns
            .into_iter()
            .map(|target| normalize_plpgsql_assignment_target(target, env))
            .collect()
    });
    stmt.source = match stmt.source {
        InsertSource::Values(rows) => InsertSource::Values(
            rows.into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| normalize_plpgsql_expr(expr, env))
                        .collect()
                })
                .collect(),
        ),
        InsertSource::DefaultValues => InsertSource::DefaultValues,
        InsertSource::Select(select) => {
            InsertSource::Select(Box::new(normalize_plpgsql_select(*select, env)))
        }
    };
    stmt.on_conflict = stmt
        .on_conflict
        .map(|clause| normalize_plpgsql_on_conflict(clause, env));
    let returning_targets = std::mem::take(&mut stmt.returning.targets);
    stmt.returning.targets = returning_targets
        .into_iter()
        .map(|item| normalize_plpgsql_select_item(item, env))
        .collect();
    stmt
}

fn normalize_plpgsql_update(mut stmt: UpdateStatement, env: &CompileEnv) -> UpdateStatement {
    stmt.with = stmt
        .with
        .into_iter()
        .map(|mut cte| {
            cte.body = normalize_plpgsql_cte_body(cte.body, env);
            cte
        })
        .collect();
    stmt.assignments = stmt
        .assignments
        .into_iter()
        .map(|assignment| normalize_plpgsql_assignment(assignment, env))
        .collect();
    stmt.from = stmt.from.map(|from| normalize_plpgsql_from_item(from, env));
    stmt.where_clause = stmt
        .where_clause
        .map(|expr| normalize_plpgsql_expr(expr, env));
    let returning_targets = std::mem::take(&mut stmt.returning.targets);
    stmt.returning.targets = returning_targets
        .into_iter()
        .map(|item| normalize_plpgsql_select_item(item, env))
        .collect();
    stmt
}

fn normalize_plpgsql_delete(mut stmt: DeleteStatement, env: &CompileEnv) -> DeleteStatement {
    stmt.with = stmt
        .with
        .into_iter()
        .map(|mut cte| {
            cte.body = normalize_plpgsql_cte_body(cte.body, env);
            cte
        })
        .collect();
    stmt.where_clause = stmt
        .where_clause
        .map(|expr| normalize_plpgsql_expr(expr, env));
    stmt.using = stmt
        .using
        .map(|from| normalize_plpgsql_from_item(from, env));
    let returning_targets = std::mem::take(&mut stmt.returning.targets);
    stmt.returning.targets = returning_targets
        .into_iter()
        .map(|item| normalize_plpgsql_select_item(item, env))
        .collect();
    stmt
}

fn normalize_plpgsql_merge(mut stmt: MergeStatement, env: &CompileEnv) -> MergeStatement {
    stmt.with = stmt
        .with
        .into_iter()
        .map(|mut cte| {
            cte.body = normalize_plpgsql_cte_body(cte.body, env);
            cte
        })
        .collect();
    stmt.source = normalize_plpgsql_from_item(stmt.source, env);
    stmt.join_condition = normalize_plpgsql_expr(stmt.join_condition, env);
    stmt.when_clauses = stmt
        .when_clauses
        .into_iter()
        .map(|mut clause| {
            clause.condition = clause
                .condition
                .map(|expr| normalize_plpgsql_expr(expr, env));
            clause.action = match clause.action {
                MergeAction::DoNothing => MergeAction::DoNothing,
                MergeAction::Delete => MergeAction::Delete,
                MergeAction::Update { assignments } => MergeAction::Update {
                    assignments: assignments
                        .into_iter()
                        .map(|assignment| normalize_plpgsql_assignment(assignment, env))
                        .collect(),
                },
                MergeAction::Insert {
                    columns,
                    overriding,
                    source,
                } => MergeAction::Insert {
                    columns: columns.map(|columns| {
                        columns
                            .into_iter()
                            .map(|target| normalize_plpgsql_assignment_target(target, env))
                            .collect()
                    }),
                    overriding,
                    source: match source {
                        MergeInsertSource::Values(values) => MergeInsertSource::Values(
                            values
                                .into_iter()
                                .map(|expr| normalize_plpgsql_expr(expr, env))
                                .collect(),
                        ),
                        MergeInsertSource::DefaultValues => MergeInsertSource::DefaultValues,
                    },
                },
            };
            clause
        })
        .collect();
    let returning_targets = std::mem::take(&mut stmt.returning.targets);
    stmt.returning.targets = returning_targets
        .into_iter()
        .map(|item| normalize_plpgsql_select_item(item, env))
        .collect();
    stmt
}

fn normalize_plpgsql_values(mut values: ValuesStatement, env: &CompileEnv) -> ValuesStatement {
    values.with = values
        .with
        .into_iter()
        .map(|mut cte| {
            cte.body = normalize_plpgsql_cte_body(cte.body, env);
            cte
        })
        .collect();
    values.rows = values
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|expr| normalize_plpgsql_expr(expr, env))
                .collect()
        })
        .collect();
    values.order_by = values
        .order_by
        .into_iter()
        .map(|mut item| {
            item.expr = normalize_plpgsql_expr(item.expr, env);
            item
        })
        .collect();
    values
}

fn normalize_plpgsql_on_conflict(
    mut clause: OnConflictClause,
    env: &CompileEnv,
) -> OnConflictClause {
    clause.target = clause.target.map(|target| match target {
        OnConflictTarget::Inference(mut inference) => {
            inference.elements = inference
                .elements
                .into_iter()
                .map(|mut elem| {
                    elem.expr = normalize_plpgsql_expr(elem.expr, env);
                    elem
                })
                .collect();
            inference.predicate = inference
                .predicate
                .map(|expr| normalize_plpgsql_expr(expr, env));
            OnConflictTarget::Inference(inference)
        }
        OnConflictTarget::Constraint(name) => OnConflictTarget::Constraint(name),
    });
    clause.assignments = clause
        .assignments
        .into_iter()
        .map(|assignment| normalize_plpgsql_assignment(assignment, env))
        .collect();
    clause.where_clause = clause
        .where_clause
        .map(|expr| normalize_plpgsql_expr(expr, env));
    clause
}

fn normalize_plpgsql_select_item(mut item: SelectItem, env: &CompileEnv) -> SelectItem {
    item.expr = normalize_plpgsql_expr(item.expr, env);
    item
}

fn normalize_plpgsql_assignment(mut assignment: Assignment, env: &CompileEnv) -> Assignment {
    assignment.target = normalize_plpgsql_assignment_target(assignment.target, env);
    assignment.expr = normalize_plpgsql_expr(assignment.expr, env);
    assignment
}

fn normalize_plpgsql_assignment_target(
    mut target: AssignmentTarget,
    env: &CompileEnv,
) -> AssignmentTarget {
    target.subscripts = target
        .subscripts
        .into_iter()
        .map(|subscript| normalize_plpgsql_array_subscript(subscript, env))
        .collect();
    target.indirection = target
        .indirection
        .into_iter()
        .map(|step| match step {
            AssignmentTargetIndirection::Subscript(subscript) => {
                AssignmentTargetIndirection::Subscript(normalize_plpgsql_array_subscript(
                    subscript, env,
                ))
            }
            AssignmentTargetIndirection::Field(field) => AssignmentTargetIndirection::Field(field),
        })
        .collect();
    target
}

fn normalize_plpgsql_from_item(item: FromItem, env: &CompileEnv) -> FromItem {
    match item {
        FromItem::Values { rows } => FromItem::Values {
            rows: rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| normalize_plpgsql_expr(expr, env))
                        .collect()
                })
                .collect(),
        },
        FromItem::FunctionCall {
            name,
            args,
            func_variadic,
            with_ordinality,
        } => FromItem::FunctionCall {
            name,
            args: args
                .into_iter()
                .map(|mut arg| {
                    arg.value = normalize_plpgsql_expr(arg.value, env);
                    arg
                })
                .collect(),
            func_variadic,
            with_ordinality,
        },
        FromItem::XmlTable(mut table) => {
            table.namespaces = table
                .namespaces
                .into_iter()
                .map(|mut namespace| {
                    namespace.uri = normalize_plpgsql_expr(namespace.uri, env);
                    namespace
                })
                .collect();
            table.row_path = normalize_plpgsql_expr(table.row_path, env);
            table.document = normalize_plpgsql_expr(table.document, env);
            table.columns = table
                .columns
                .into_iter()
                .map(|column| match column {
                    XmlTableColumn::Regular {
                        name,
                        type_name,
                        path,
                        default,
                        not_null,
                    } => XmlTableColumn::Regular {
                        name,
                        type_name,
                        path: path.map(|expr| normalize_plpgsql_expr(expr, env)),
                        default: default.map(|expr| normalize_plpgsql_expr(expr, env)),
                        not_null,
                    },
                    XmlTableColumn::Ordinality { name } => XmlTableColumn::Ordinality { name },
                })
                .collect();
            FromItem::XmlTable(table)
        }
        FromItem::Lateral(source) => {
            FromItem::Lateral(Box::new(normalize_plpgsql_from_item(*source, env)))
        }
        FromItem::DerivedTable(select) => {
            FromItem::DerivedTable(Box::new(normalize_plpgsql_select(*select, env)))
        }
        FromItem::Join {
            left,
            right,
            kind,
            constraint,
        } => FromItem::Join {
            left: Box::new(normalize_plpgsql_from_item(*left, env)),
            right: Box::new(normalize_plpgsql_from_item(*right, env)),
            kind,
            constraint: match constraint {
                crate::backend::parser::JoinConstraint::On(expr) => {
                    crate::backend::parser::JoinConstraint::On(normalize_plpgsql_expr(expr, env))
                }
                other => other,
            },
        },
        FromItem::Alias {
            source,
            alias,
            column_aliases,
            preserve_source_names,
        } => FromItem::Alias {
            source: Box::new(normalize_plpgsql_from_item(*source, env)),
            alias,
            column_aliases,
            preserve_source_names,
        },
        other => other,
    }
}

fn seed_trigger_env(env: &mut CompileEnv, relation_desc: &RelationDesc) -> CompiledTriggerBindings {
    let new_row = env.define_trigger_relation_scope("new", relation_desc, TriggerReturnedRow::New);
    let old_row = env.define_trigger_relation_scope("old", relation_desc, TriggerReturnedRow::Old);
    let tg_name_slot = env.define_var("tg_name", SqlType::new(SqlTypeKind::Text));
    let tg_op_slot = env.define_var("tg_op", SqlType::new(SqlTypeKind::Text));
    let tg_when_slot = env.define_var("tg_when", SqlType::new(SqlTypeKind::Text));
    let tg_level_slot = env.define_var("tg_level", SqlType::new(SqlTypeKind::Text));
    let tg_relid_slot = env.define_var("tg_relid", SqlType::new(SqlTypeKind::Oid));
    let tg_nargs_slot = env.define_var("tg_nargs", SqlType::new(SqlTypeKind::Int4));
    let tg_argv_slot = env.define_var(
        "tg_argv",
        SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
    );
    let tg_table_name_slot = env.define_var("tg_table_name", SqlType::new(SqlTypeKind::Text));
    env.define_alias(
        "tg_relname",
        tg_table_name_slot,
        SqlType::new(SqlTypeKind::Text),
    );
    let tg_table_schema_slot = env.define_var("tg_table_schema", SqlType::new(SqlTypeKind::Text));

    CompiledTriggerBindings {
        new_row,
        old_row,
        tg_name_slot,
        tg_op_slot,
        tg_when_slot,
        tg_level_slot,
        tg_relid_slot,
        tg_nargs_slot,
        tg_argv_slot,
        tg_table_name_slot,
        tg_table_schema_slot,
    }
}

fn seed_event_trigger_env(env: &mut CompileEnv) -> CompiledEventTriggerBindings {
    let tg_event_slot = env.define_var("tg_event", SqlType::new(SqlTypeKind::Text));
    let tg_tag_slot = env.define_var("tg_tag", SqlType::new(SqlTypeKind::Text));
    CompiledEventTriggerBindings {
        tg_event_slot,
        tg_tag_slot,
    }
}

fn resolve_assign_target(
    target: &AssignTarget,
    env: &CompileEnv,
) -> Result<(usize, SqlType, Option<String>, bool), ParseError> {
    match target {
        AssignTarget::Name(name) => env
            .get_var(name)
            .map(|var| {
                if var.constant {
                    Err(ParseError::DetailedError {
                        message: format!("variable \"{name}\" is declared CONSTANT"),
                        detail: None,
                        hint: None,
                        sqlstate: "22005",
                    })
                } else {
                    Ok((var.slot, var.ty, Some(name.clone()), var.not_null))
                }
            })
            .transpose()?
            .ok_or_else(|| ParseError::UnknownColumn(name.clone())),
        AssignTarget::Parameter(index) => env
            .get_parameter(*index)
            .map(|var| (var.slot, var.ty, None, false))
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "existing positional parameter reference",
                actual: format!("${index}"),
            }),
        AssignTarget::Field { relation, field } => env
            .get_relation_field(relation, field)
            .map(|column| (column.slot, column.sql_type, None, false))
            .ok_or_else(|| ParseError::UnknownColumn(format!("{relation}.{field}"))),
        AssignTarget::Subscript { name, .. } => env
            .get_var(name)
            .map(|var| (var.slot, var.ty, Some(name.clone()), var.not_null))
            .ok_or_else(|| ParseError::UnknownColumn(name.clone())),
        AssignTarget::FieldSubscript {
            relation, field, ..
        } => env
            .get_relation_field(relation, field)
            .map(|column| (column.slot, column.sql_type, None, false))
            .or_else(|| {
                env.get_var(relation)
                    .map(|var| (var.slot, var.ty, None, false))
            })
            .ok_or_else(|| ParseError::UnknownColumn(format!("{relation}.{field}"))),
    }
}

fn compile_indirect_assign_target(
    target: &AssignTarget,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<Option<CompiledIndirectAssignTarget>, ParseError> {
    match target {
        AssignTarget::Field { relation, field } => {
            let Some(var) = env.get_var(relation) else {
                return Ok(None);
            };
            Ok(Some(CompiledIndirectAssignTarget {
                slot: var.slot,
                ty: var.ty,
                indirection: vec![CompiledAssignIndirection::Field(field.clone())],
            }))
        }
        AssignTarget::FieldSubscript {
            relation,
            field,
            subscripts,
        } => {
            if let Some(var) = env.get_var(relation) {
                let mut indirection = Vec::with_capacity(subscripts.len() + 1);
                indirection.push(CompiledAssignIndirection::Field(field.clone()));
                indirection.extend(
                    subscripts
                        .iter()
                        .map(|subscript| {
                            compile_expr_text(subscript, catalog, env)
                                .map(CompiledAssignIndirection::Subscript)
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                );
                return Ok(Some(CompiledIndirectAssignTarget {
                    slot: var.slot,
                    ty: var.ty,
                    indirection,
                }));
            }

            let column = env
                .get_relation_field(relation, field)
                .ok_or_else(|| ParseError::UnknownColumn(format!("{relation}.{field}")))?;
            Ok(Some(CompiledIndirectAssignTarget {
                slot: column.slot,
                ty: column.sql_type,
                indirection: subscripts
                    .iter()
                    .map(|subscript| {
                        compile_expr_text(subscript, catalog, env)
                            .map(CompiledAssignIndirection::Subscript)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            }))
        }
        _ => Ok(None),
    }
}

fn subscripted_assignment_target_type(
    root_ty: SqlType,
    subscript_count: usize,
    catalog: &dyn CatalogLookup,
) -> Result<SqlType, ParseError> {
    let ty = plpgsql_assignment_navigation_sql_type(root_ty, catalog);
    if subscript_count == 0 {
        return Ok(ty);
    }
    if !ty.is_array {
        return Err(ParseError::DetailedError {
            message: format!(
                "cannot subscript type {} because it does not support subscripting",
                crate::backend::parser::analyze::sql_type_name(ty)
            ),
            detail: None,
            hint: None,
            sqlstate: "42804",
        });
    }
    Ok(plpgsql_assignment_navigation_sql_type(
        ty.element_type(),
        catalog,
    ))
}

fn plpgsql_assignment_navigation_sql_type(mut ty: SqlType, catalog: &dyn CatalogLookup) -> SqlType {
    loop {
        let Some(domain) = catalog.domain_by_type_oid(ty.type_oid) else {
            return ty;
        };
        if ty.is_array && !domain.sql_type.is_array {
            return SqlType::array_of(domain.sql_type);
        }
        ty = domain.sql_type;
    }
}

fn compile_select_into_target(
    target: &AssignTarget,
    env: &CompileEnv,
) -> Result<CompiledSelectIntoTarget, ParseError> {
    let (slot, ty, name, not_null) = resolve_assign_target(target, env)?;
    Ok(CompiledSelectIntoTarget {
        slot,
        ty,
        name,
        not_null,
    })
}

fn parse_proc_argtype_oids(argtypes: &str) -> Option<Vec<u32>> {
    if argtypes.trim().is_empty() {
        return Some(Vec::new());
    }
    argtypes
        .split_whitespace()
        .map(|part| part.parse::<u32>().ok())
        .collect()
}

#[allow(dead_code)]
pub(crate) fn compile_decl_type(type_name: &str) -> Result<SqlType, ParseError> {
    parse_type_name(type_name).and_then(|ty| match ty {
        crate::backend::parser::RawTypeName::Builtin(sql_type) => Ok(sql_type),
        crate::backend::parser::RawTypeName::Serial(kind) => {
            Err(ParseError::FeatureNotSupported(format!(
                "{} is only allowed in CREATE TABLE / ALTER TABLE ADD COLUMN",
                match kind {
                    crate::backend::parser::SerialKind::Small => "smallserial",
                    crate::backend::parser::SerialKind::Regular => "serial",
                    crate::backend::parser::SerialKind::Big => "bigserial",
                }
            )))
        }
        crate::backend::parser::RawTypeName::Record => {
            Err(ParseError::UnsupportedType("record".into()))
        }
        crate::backend::parser::RawTypeName::Named { name, .. } => {
            Err(ParseError::UnsupportedType(name))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::BoundRelation;

    struct EmptyCatalog;

    impl CatalogLookup for EmptyCatalog {
        fn lookup_any_relation(&self, _name: &str) -> Option<BoundRelation> {
            None
        }
    }

    #[test]
    fn rewrites_plpgsql_count_condition_with_from_clause() {
        assert_eq!(
            rewrite_plpgsql_query_condition("count(*) = 0 from Room where roomno = new.roomno"),
            Some("(select count(*) from Room where roomno = new.roomno) = 0".into())
        );
    }

    #[test]
    fn normalizes_labeled_record_field_reference() {
        let mut env = CompileEnv::default();
        let slot = env.define_var("item", SqlType::record(RECORD_TYPE_OID));
        env.push_label_scope("outer");

        let parsed = parse_expr("\"outer\".item.note").unwrap();
        assert_eq!(
            normalize_plpgsql_expr(parsed, &env),
            SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column(plpgsql_label_alias(0, slot, "item"))),
                field: "note".into(),
            }
        );
    }

    #[test]
    fn normalizes_labeled_scalar_variable_reference() {
        let mut env = CompileEnv::default();
        let slot = env.define_var("param1", SqlType::new(SqlTypeKind::Int4));
        env.push_label_scope("pl_qual_names");
        env.define_var("param1", SqlType::new(SqlTypeKind::Int4));

        let parsed = parse_expr("pl_qual_names.param1").unwrap();
        assert_eq!(
            normalize_plpgsql_expr(parsed, &env),
            SqlExpr::Column(plpgsql_label_alias(0, slot, "param1")),
        );
    }

    #[test]
    fn labeled_record_field_reference_survives_inner_shadowing() {
        let mut env = CompileEnv::default();
        let outer_slot = env.define_var("rec", SqlType::record(RECORD_TYPE_OID));
        env.push_label_scope("outer");
        env.define_var("rec", SqlType::record(RECORD_TYPE_OID));

        let parsed = parse_expr("\"outer\".rec.backlink").unwrap();
        assert_eq!(
            normalize_plpgsql_expr(parsed, &env),
            SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column(plpgsql_label_alias(0, outer_slot, "rec"))),
                field: "backlink".into(),
            }
        );
    }

    #[test]
    fn normalizes_record_field_references_in_insert_values() {
        let mut env = CompileEnv::default();
        env.define_var("obj", SqlType::record(RECORD_TYPE_OID));

        let Statement::Insert(stmt) =
            parse_statement("insert into dropped_objects (object_type) values (obj.object_type)")
                .unwrap()
        else {
            panic!("expected INSERT statement");
        };
        let normalized = normalize_plpgsql_insert(stmt, &env);

        let InsertSource::Values(rows) = normalized.source else {
            panic!("expected INSERT VALUES source");
        };
        assert_eq!(
            rows[0][0],
            SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column("obj".into())),
                field: "object_type".into(),
            }
        );
    }

    #[test]
    fn rewrites_plpgsql_count_condition_with_greater_than() {
        assert_eq!(
            rewrite_plpgsql_query_condition("count(*) > 0 from Hub where name = old.hubname"),
            Some("(select count(*) from Hub where name = old.hubname) > 0".into())
        );
    }

    #[test]
    fn rewrites_plpgsql_assignment_query_expr() {
        assert_eq!(
            rewrite_plpgsql_assignment_query_expr(
                "retval || slotno::text from HSlot where slotname = psrec.slotlink"
            ),
            Some(
                "(select retval || slotno::text from HSlot where slotname = psrec.slotlink)".into()
            )
        );
    }

    #[test]
    fn ignores_normal_scalar_conditions() {
        assert_eq!(
            rewrite_plpgsql_query_condition("new.slotno < 1 or new.slotno > hubrec.nslots"),
            None
        );
    }

    #[test]
    fn aliases_trigger_relation_scope() {
        let mut env = CompileEnv::default();
        let desc = RelationDesc {
            columns: Vec::new(),
        };
        let bindings = seed_trigger_env(&mut env, &desc);

        assert!(env.define_relation_alias("ps", TriggerReturnedRow::New));
        assert_eq!(
            env.trigger_relation_return_row("ps"),
            Some(TriggerReturnedRow::New)
        );

        let stmt = compile_return_stmt(
            Some("ps"),
            1,
            &EmptyCatalog,
            &env,
            Some(&FunctionReturnContract::Trigger { bindings }),
        )
        .unwrap();
        assert!(matches!(
            stmt,
            CompiledStmt::ReturnTriggerRow {
                row: TriggerReturnedRow::New
            }
        ));
    }
}
