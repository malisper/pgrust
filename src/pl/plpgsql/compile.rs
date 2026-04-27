#![allow(dead_code)]

use std::collections::HashMap;

use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::Expr;
use crate::backend::executor::RelationDesc;
use crate::backend::optimizer::finalize_expr_subqueries;
use crate::backend::parser::analyze::scope_for_relation;
use crate::backend::parser::{
    ArraySubscript, Assignment, AssignmentTarget, AssignmentTargetIndirection, BoundCte,
    BoundDeleteStatement, BoundInsertStatement, BoundUpdateStatement, CatalogLookup, CteBody,
    FromItem, InsertSource, InsertStatement, OnConflictClause, OnConflictTarget, OrderByItem,
    ParseError, RawWindowFrame, RawWindowFrameBound, RawWindowSpec, RawXmlExpr, SelectItem,
    SelectStatement, SlotScopeColumn, SqlCallArgs, SqlCaseWhen, SqlExpr, SqlType, SqlTypeKind,
    Statement, ValuesStatement, bind_delete_with_outer_scopes, bind_insert_with_outer_scopes,
    bind_scalar_expr_in_named_slot_scope, bind_update_with_outer_scopes, parse_expr,
    parse_statement, parse_type_name, pg_plan_query_with_outer_scopes_and_ctes,
    pg_plan_values_query_with_outer_scopes_and_ctes, resolve_raw_type_name,
};
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::include::catalog::{PgProcRow, RECORD_TYPE_OID};
use crate::include::nodes::plannodes::{Plan, PlannedStmt};
use crate::include::nodes::primnodes::{QueryColumn, TargetEntry, Var, user_attrno};

use super::ast::{
    AliasTarget, AssignTarget, Block, CursorDecl, Decl, ExceptionCondition, ForQuerySource,
    ForTarget, RaiseLevel, ReturnQueryKind, Stmt, VarDecl,
};
use super::gram::parse_block;

#[derive(Debug, Clone)]
pub(crate) struct CompiledBlock {
    pub(crate) local_slots: Vec<CompiledVar>,
    pub(crate) statements: Vec<CompiledStmt>,
    pub(crate) exception_handlers: Vec<CompiledExceptionHandler>,
    pub(crate) total_slots: usize,
}

#[derive(Debug, Clone)]
pub struct CompiledFunction {
    pub(crate) name: String,
    pub(crate) parameter_slots: Vec<CompiledFunctionSlot>,
    pub(crate) output_slots: Vec<CompiledOutputSlot>,
    pub(crate) body: CompiledBlock,
    pub(crate) return_contract: FunctionReturnContract,
    pub(crate) found_slot: usize,
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
    pub(crate) slot: usize,
    pub(crate) ty: SqlType,
    pub(crate) default_expr: Option<CompiledExpr>,
}

#[derive(Debug, Clone)]
pub(crate) enum CompiledExpr {
    Scalar {
        expr: Expr,
        subplans: Vec<Plan>,
    },
    QueryCompare {
        plan: PlannedStmt,
        op: QueryCompareOp,
        rhs: Expr,
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

#[derive(Debug, Clone, Copy)]
pub(crate) struct CompiledSelectIntoTarget {
    pub(crate) slot: usize,
    pub(crate) ty: SqlType,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledForQueryTarget {
    pub(crate) targets: Vec<CompiledSelectIntoTarget>,
}

#[derive(Debug, Clone)]
pub(crate) enum CompiledForQuerySource {
    Static {
        plan: PlannedStmt,
    },
    Dynamic {
        sql_expr: CompiledExpr,
        using_exprs: Vec<CompiledExpr>,
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
    },
    AnonymousRecord {
        setof: bool,
    },
    Trigger {
        bindings: CompiledTriggerBindings,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TriggerReturnedRow {
    New,
    Old,
}

#[derive(Debug, Clone)]
pub(crate) enum CompiledStmt {
    Block(CompiledBlock),
    Assign {
        slot: usize,
        ty: SqlType,
        expr: CompiledExpr,
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
    Raise {
        level: RaiseLevel,
        sqlstate: Option<String>,
        message: String,
        params: Vec<CompiledExpr>,
    },
    Assert {
        condition: CompiledExpr,
        message: Option<CompiledExpr>,
    },
    Return {
        expr: Option<CompiledExpr>,
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
        plan: PlannedStmt,
        kind: ReturnQueryKind,
    },
    Perform {
        plan: PlannedStmt,
    },
    DynamicExecute {
        sql_expr: CompiledExpr,
        into_targets: Vec<CompiledSelectIntoTarget>,
        using_exprs: Vec<CompiledExpr>,
    },
    GetDiagnostics {
        stacked: bool,
        items: Vec<(CompiledSelectIntoTarget, String)>,
    },
    OpenCursor {
        slot: usize,
        name: String,
        plan: PlannedStmt,
    },
    FetchCursor {
        slot: usize,
        targets: Vec<CompiledSelectIntoTarget>,
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
}

#[derive(Debug, Clone)]
struct ScopeVar {
    slot: usize,
    ty: SqlType,
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
    vars: HashMap<String, ScopeVar>,
    relation_scopes: Vec<RelationScopeVar>,
}

#[derive(Debug, Clone, Default)]
struct CompileEnv {
    vars: HashMap<String, ScopeVar>,
    relation_scopes: Vec<RelationScopeVar>,
    labeled_scopes: Vec<LabeledScope>,
    local_ctes: Vec<BoundCte>,
    declared_cursors: HashMap<String, String>,
    parameter_slots: Vec<ScopeVar>,
    positional_parameter_names: Vec<String>,
    next_slot: usize,
}

impl CompileEnv {
    fn child(&self) -> Self {
        self.clone()
    }

    fn define_var(&mut self, name: &str, ty: SqlType) -> usize {
        let slot = self.next_slot;
        self.next_slot += 1;
        self.vars
            .insert(name.to_ascii_lowercase(), ScopeVar { slot, ty });
        slot
    }

    fn define_parameter_var(&mut self, name: &str, ty: SqlType) -> usize {
        let slot = self.define_var(name, ty);
        self.parameter_slots.push(ScopeVar { slot, ty });
        let positional_name = positional_parameter_var_name(self.parameter_slots.len());
        self.vars
            .insert(positional_name.clone(), ScopeVar { slot, ty });
        self.positional_parameter_names.push(positional_name);
        slot
    }

    fn define_alias(&mut self, name: &str, slot: usize, ty: SqlType) {
        self.vars
            .insert(name.to_ascii_lowercase(), ScopeVar { slot, ty });
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
    }

    fn get_var(&self, name: &str) -> Option<&ScopeVar> {
        self.vars.get(&name.to_ascii_lowercase())
    }

    fn get_labeled_var(&self, label: &str, name: &str) -> Option<&ScopeVar> {
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
        self.labeled_scopes.push(LabeledScope {
            label: label.to_ascii_lowercase(),
            vars: self.vars.clone(),
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

    fn define_cursor(&mut self, name: &str, query: &str) {
        self.declared_cursors
            .insert(name.to_ascii_lowercase(), query.to_string());
    }

    fn declared_cursor_query(&self, name: &str) -> Option<&str> {
        self.declared_cursors
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
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
        ordered.sort_by_key(|column| column.slot);
        ordered
    }

    fn relation_slot_scopes(&self) -> Vec<(String, Vec<SlotScopeColumn>)> {
        self.relation_scopes
            .iter()
            .map(|scope| (scope.name.clone(), scope.columns.clone()))
            .collect()
    }
}

pub(crate) fn compile_do_block(
    block: &Block,
    catalog: &dyn CatalogLookup,
) -> Result<CompiledBlock, ParseError> {
    let mut env = CompileEnv::default();
    let _ = env.define_var("found", SqlType::new(SqlTypeKind::Bool));
    compile_block(block, catalog, &mut env, None)
}

pub(crate) fn compile_do_function(
    block: &Block,
    catalog: &dyn CatalogLookup,
) -> Result<CompiledFunction, ParseError> {
    let mut env = CompileEnv::default();
    let found_slot = env.define_var("found", SqlType::new(SqlTypeKind::Bool));
    let sqlerrm_slot = env.define_var("sqlerrm", SqlType::new(SqlTypeKind::Text));
    let return_contract = FunctionReturnContract::Scalar {
        ty: SqlType::new(SqlTypeKind::Void),
        setof: false,
        output_slot: None,
    };
    let body = compile_block(block, catalog, &mut env, Some(&return_contract))?;
    Ok(CompiledFunction {
        name: "inline_code_block".into(),
        parameter_slots: Vec::new(),
        output_slots: Vec::new(),
        body,
        return_contract,
        found_slot,
        sqlerrm_slot,
        local_ctes: Vec::new(),
        trigger_transition_ctes: Vec::new(),
    })
}

pub(crate) fn compile_function_from_proc(
    row: &PgProcRow,
    catalog: &dyn CatalogLookup,
) -> Result<CompiledFunction, ParseError> {
    let block = parse_block(&row.prosrc)?;
    let mut env = CompileEnv::default();
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
                b'i' => {
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
    let sqlerrm_slot = env.define_var("sqlerrm", SqlType::new(SqlTypeKind::Text));

    let return_contract = function_return_contract(row, catalog, &output_slots)?;
    let body = compile_block(&block, catalog, &mut env, Some(&return_contract))?;
    Ok(CompiledFunction {
        name: row.proname.clone(),
        parameter_slots,
        output_slots,
        body,
        return_contract,
        found_slot,
        sqlerrm_slot,
        local_ctes: Vec::new(),
        trigger_transition_ctes: Vec::new(),
    })
}

pub(crate) fn compile_trigger_function_from_proc(
    row: &PgProcRow,
    relation_desc: &RelationDesc,
    transition_tables: &[TriggerTransitionTable],
    catalog: &dyn CatalogLookup,
) -> Result<CompiledFunction, ParseError> {
    let block = parse_block(&row.prosrc)?;
    let mut env = CompileEnv::default();
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
    let sqlerrm_slot = env.define_var("sqlerrm", SqlType::new(SqlTypeKind::Text));
    let return_contract = FunctionReturnContract::Trigger { bindings };
    let body = compile_block(&block, catalog, &mut env, Some(&return_contract))?;
    Ok(CompiledFunction {
        name: row.proname.clone(),
        parameter_slots: Vec::new(),
        output_slots: Vec::new(),
        body,
        return_contract,
        found_slot,
        sqlerrm_slot,
        local_ctes: env.local_ctes.clone(),
        trigger_transition_ctes,
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
            Ok(CompiledExceptionHandler {
                conditions: handler.conditions.clone(),
                statements: compile_stmt_list(
                    &handler.statements,
                    catalog,
                    &mut env,
                    return_contract,
                )?,
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    outer.next_slot = outer.next_slot.max(env.next_slot);
    Ok(CompiledBlock {
        local_slots,
        statements,
        exception_handlers,
        total_slots: outer.next_slot,
    })
}

fn compile_var_decl(
    decl: &VarDecl,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledVar, ParseError> {
    let ty = resolve_decl_type(&decl.type_name, catalog)?;
    let slot = env.define_var(&decl.name, ty);
    let default_expr = decl
        .default_expr
        .as_deref()
        .map(|expr| compile_expr_text(expr, catalog, env))
        .transpose()?;
    Ok(CompiledVar {
        slot,
        ty,
        default_expr,
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
    env.define_cursor(&decl.name, &decl.query);
    Ok(CompiledVar {
        slot,
        ty,
        default_expr: Some(compile_expr_text(
            &format!("'{}'", decl.name.replace('\'', "''")),
            catalog,
            env,
        )?),
    })
}

fn resolve_decl_type(type_name: &str, catalog: &dyn CatalogLookup) -> Result<SqlType, ParseError> {
    let trimmed = type_name.trim();
    let lowered = trimmed.to_ascii_lowercase();
    if let Some(prefix) = lowered.strip_suffix("%type") {
        let original_prefix = &trimmed[..prefix.len()];
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
        Stmt::Block(block) => {
            CompiledStmt::Block(compile_block(block, catalog, env, return_contract)?)
        }
        Stmt::Assign { target, expr } => {
            let (slot, ty) = resolve_assign_target(target, env)?;
            CompiledStmt::Assign {
                slot,
                ty,
                expr: compile_expr_text(expr, catalog, env)?,
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
        Stmt::Raise {
            level,
            sqlstate,
            message,
            params,
        } => {
            let placeholder_count = message.matches('%').count();
            if placeholder_count != params.len() {
                return Err(ParseError::UnexpectedToken {
                    expected: "RAISE placeholder count matching argument count",
                    actual: format!(
                        "message has {placeholder_count} placeholders but {} arguments were provided",
                        params.len()
                    ),
                });
            }
            CompiledStmt::Raise {
                level: level.clone(),
                sqlstate: sqlstate.clone(),
                message: message.clone(),
                params: params
                    .iter()
                    .map(|expr| compile_expr_text(expr, catalog, env))
                    .collect::<Result<_, _>>()?,
            }
        }
        Stmt::Assert { condition, message } => CompiledStmt::Assert {
            condition: compile_condition_text(condition, catalog, env)?,
            message: message
                .as_deref()
                .map(|expr| compile_expr_text(expr, catalog, env))
                .transpose()?,
        },
        Stmt::Return { expr } => {
            compile_return_stmt(expr.as_deref(), catalog, env, return_contract)?
        }
        Stmt::ReturnNext { expr } => {
            compile_return_next_stmt(expr.as_deref(), catalog, env, return_contract)?
        }
        Stmt::ReturnQuery { sql, kind } => {
            compile_return_query_stmt(sql, *kind, catalog, env, return_contract)?
        }
        Stmt::Perform { sql } => compile_perform_stmt(sql, catalog, env)?,
        Stmt::DynamicExecute {
            sql_expr,
            into_targets,
            using_exprs,
        } => compile_dynamic_execute_stmt(sql_expr, into_targets, using_exprs, catalog, env)?,
        Stmt::GetDiagnostics { stacked, items } => {
            let items = items
                .iter()
                .map(|(target, item)| {
                    let (slot, ty) = resolve_assign_target(target, env)?;
                    Ok((CompiledSelectIntoTarget { slot, ty }, item.clone()))
                })
                .collect::<Result<Vec<_>, ParseError>>()?;
            CompiledStmt::GetDiagnostics {
                stacked: *stacked,
                items,
            }
        }
        Stmt::OpenCursor { name, sql } => {
            compile_open_cursor_stmt(name, sql.as_deref(), catalog, env)?
        }
        Stmt::FetchCursor { name, targets } => {
            let (slot, _) = resolve_assign_target(&AssignTarget::Name(name.clone()), env)?;
            let targets = targets
                .iter()
                .map(|target| {
                    resolve_assign_target(target, env)
                        .map(|(slot, ty)| CompiledSelectIntoTarget { slot, ty })
                })
                .collect::<Result<Vec<_>, _>>()?;
            CompiledStmt::FetchCursor { slot, targets }
        }
        Stmt::CloseCursor { name } => {
            let (slot, _) = resolve_assign_target(&AssignTarget::Name(name.clone()), env)?;
            CompiledStmt::CloseCursor { slot }
        }
        Stmt::ExecSql { sql } => compile_exec_sql_stmt(sql, catalog, env)?,
    })
}

fn compile_return_stmt(
    expr: Option<&str>,
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
        (FunctionReturnContract::Scalar { setof: false, .. }, Some(expr)) => {
            Ok(CompiledStmt::Return {
                expr: Some(compile_expr_text(expr, catalog, env)?),
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
            Ok(CompiledStmt::Return { expr: None })
        }
        (FunctionReturnContract::FixedRow { .. }, None)
        | (FunctionReturnContract::AnonymousRecord { .. }, None) => {
            Ok(CompiledStmt::Return { expr: None })
        }
        (
            FunctionReturnContract::FixedRow { setof: false, .. }
            | FunctionReturnContract::AnonymousRecord { setof: false },
            Some(expr),
        ) => Ok(CompiledStmt::Return {
            expr: Some(compile_expr_text(expr, catalog, env)?),
        }),
        _ => Err(ParseError::FeatureNotSupported(
            "RETURN expr is only supported for scalar function returns".into(),
        )),
    }
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
        (FunctionReturnContract::Trigger { .. }, _) => Err(ParseError::FeatureNotSupported(
            "RETURN NEXT is not valid in trigger functions".into(),
        )),
        (FunctionReturnContract::Scalar { setof: true, .. }, Some(expr)) => {
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
    let stmt = normalize_plpgsql_select(stmt.clone(), env);
    pg_plan_query_with_outer_scopes_and_ctes(
        &stmt,
        catalog,
        &[outer_scope_for_sql(env)],
        &env.local_ctes,
    )
}

fn plan_values_for_env(
    stmt: &crate::backend::parser::ValuesStatement,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<PlannedStmt, ParseError> {
    pg_plan_values_query_with_outer_scopes_and_ctes(
        stmt,
        catalog,
        &[outer_scope_for_sql(env)],
        &env.local_ctes,
    )
}

fn compile_return_query_stmt(
    sql: &str,
    kind: ReturnQueryKind,
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
        FunctionReturnContract::Trigger { .. } => false,
    };
    if !is_setof {
        return Err(ParseError::FeatureNotSupported(
            "RETURN QUERY requires a set-returning function".into(),
        ));
    }

    let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
    let planned = match parse_statement(&rewritten_sql)? {
        Statement::Select(stmt) => plan_select_for_env(&stmt, catalog, env)?,
        Statement::Values(stmt) => plan_values_for_env(&stmt, catalog, env)?,
        other => {
            return Err(ParseError::UnexpectedToken {
                expected: "RETURN QUERY SELECT ... or RETURN QUERY VALUES (...)",
                actual: format!("{other:?}"),
            });
        }
    };
    Ok(CompiledStmt::ReturnQuery {
        plan: planned,
        kind,
    })
}

fn compile_perform_stmt(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
    let planned = plan_select_for_env(
        &crate::backend::parser::parse_select(&format!("select {rewritten_sql}"))?,
        catalog,
        env,
    )?;
    Ok(CompiledStmt::Perform { plan: planned })
}

fn compile_dynamic_execute_stmt(
    sql_expr: &str,
    into_targets: &[AssignTarget],
    using_exprs: &[String],
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    let targets = into_targets
        .iter()
        .map(|target| {
            resolve_assign_target(target, env)
                .map(|(slot, ty)| CompiledSelectIntoTarget { slot, ty })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CompiledStmt::DynamicExecute {
        sql_expr: compile_expr_text(sql_expr, catalog, env)?,
        into_targets: targets,
        using_exprs: using_exprs
            .iter()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .collect::<Result<_, _>>()?,
    })
}

fn compile_open_cursor_stmt(
    name: &str,
    sql: Option<&str>,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    let (slot, _) = resolve_assign_target(&AssignTarget::Name(name.to_string()), env)?;
    let query_sql = match sql {
        Some(sql) => sql,
        None => env
            .declared_cursor_query(name)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "declared cursor query or OPEN cursor FOR query",
                actual: name.to_string(),
            })?,
    };
    let rewritten_sql = rewrite_plpgsql_sql_text(query_sql, env)?;
    let stmt = parse_statement(&rewritten_sql)?;
    let plan = match stmt {
        Statement::Select(stmt) => plan_select_for_env(&stmt, catalog, env)?,
        Statement::Values(stmt) => plan_values_for_env(&stmt, catalog, env)?,
        other => {
            return Err(ParseError::UnexpectedToken {
                expected: "cursor query",
                actual: format!("{other:?}"),
            });
        }
    };
    Ok(CompiledStmt::OpenCursor {
        slot,
        name: name.to_string(),
        plan,
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
        return compile_select_into_stmt(
            &select_sql,
            &[AssignTarget::Name(target_name)],
            false,
            catalog,
            env,
        );
    }

    if let Some((target_names, select_sql, strict)) = split_select_with_into_targets(sql) {
        let targets = target_names
            .iter()
            .map(|target| parse_select_into_assign_target(target))
            .collect::<Result<Vec<_>, _>>()?;
        return compile_select_into_stmt(&select_sql, &targets, strict, catalog, env);
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
    let stmt = parse_statement(&rewritten_sql)?;
    let outer_scope = outer_scope_for_sql(env);
    match stmt {
        Statement::Select(stmt) => Ok(CompiledStmt::Perform {
            plan: pg_plan_query_with_outer_scopes_and_ctes(
                &stmt,
                catalog,
                &[outer_scope],
                &env.local_ctes,
            )?,
        }),
        Statement::Values(stmt) => Ok(CompiledStmt::Perform {
            plan: pg_plan_values_query_with_outer_scopes_and_ctes(
                &stmt,
                catalog,
                &[outer_scope],
                &env.local_ctes,
            )?,
        }),
        Statement::Insert(stmt) => Ok(CompiledStmt::ExecInsert {
            stmt: bind_insert_with_outer_scopes(&stmt, catalog, &[outer_scope])?,
        }),
        Statement::Update(stmt) => Ok(CompiledStmt::ExecUpdate {
            stmt: bind_update_with_outer_scopes(&stmt, catalog, &[outer_scope])?,
        }),
        Statement::Delete(stmt) => Ok(CompiledStmt::ExecDelete {
            stmt: bind_delete_with_outer_scopes(&stmt, catalog, &[outer_scope])?,
        }),
        Statement::Set(stmt) if stmt.name.eq_ignore_ascii_case("jit") => {
            // :HACK: pgrust has no JIT subsystem; PL/pgSQL regression helpers
            // use SET LOCAL jit=0 only to stabilize EXPLAIN.
            Ok(CompiledStmt::Null)
        }
        other => Err(ParseError::UnexpectedToken {
            expected: "PL/pgSQL SQL statement",
            actual: format!("{other:?}"),
        }),
    }
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

fn outer_scope_for_sql(env: &CompileEnv) -> crate::backend::parser::BoundScope {
    let columns = env.slot_columns();
    let relation_scopes = env.relation_slot_scopes();
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
    let mut output_exprs = columns
        .iter()
        .map(|column| {
            Expr::Var(Var {
                varno: 1,
                varattno: user_attrno(column.slot),
                varlevelsup: 0,
                vartype: column.sql_type,
            })
        })
        .collect::<Vec<_>>();
    let mut scope_columns = columns
        .into_iter()
        .map(|column| crate::backend::parser::analyze::ScopeColumn {
            output_name: column.name,
            hidden: column.hidden,
            qualified_only: false,
            relation_names: Vec::new(),
            hidden_invalid_relation_names: Vec::new(),
            hidden_missing_relation_names: Vec::new(),
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
            output_exprs.push(Expr::Var(Var {
                varno: 1,
                varattno: user_attrno(column.slot),
                varlevelsup: 0,
                vartype: column.sql_type,
            }));
        }
        scope_columns.extend(relation_scope.columns);
    }
    crate::backend::parser::BoundScope {
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
        || lower.starts_with("delete "))
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
        .map(|target| {
            resolve_assign_target(target, env)
                .map(|(slot, ty)| CompiledSelectIntoTarget { slot, ty })
        })
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
    })
}

fn compile_for_query_stmt(
    target: &ForTarget,
    source: &ForQuerySource,
    body: &[Stmt],
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<CompiledStmt, ParseError> {
    let (target, source) = match source {
        ForQuerySource::Static(sql) => {
            let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
            let stmt = parse_statement(&rewritten_sql)?;
            let plan = match stmt {
                Statement::Select(stmt) => plan_select_for_env(&stmt, catalog, env)?,
                Statement::Values(stmt) => plan_values_for_env(&stmt, catalog, env)?,
                other => {
                    return Err(ParseError::UnexpectedToken {
                        expected: "FOR ... IN query LOOP supports SELECT or VALUES; use EXECUTE for dynamic SQL",
                        actual: format!("{other:?}"),
                    });
                }
            };
            let target = compile_for_query_target(target, env, Some(&plan))?;
            (target, CompiledForQuerySource::Static { plan })
        }
        ForQuerySource::Execute {
            sql_expr,
            using_exprs,
        } => (
            compile_for_query_target(target, env, None)?,
            CompiledForQuerySource::Dynamic {
                sql_expr: compile_expr_text(sql_expr, catalog, env)?,
                using_exprs: using_exprs
                    .iter()
                    .map(|expr| compile_expr_text(expr, catalog, env))
                    .collect::<Result<Vec<_>, _>>()?,
            },
        ),
    };
    let body = compile_stmt_list(body, catalog, env, return_contract)?;
    Ok(CompiledStmt::ForQuery {
        target,
        source,
        body,
    })
}

fn compile_for_query_target(
    target: &ForTarget,
    env: &mut CompileEnv,
    static_plan: Option<&PlannedStmt>,
) -> Result<CompiledForQueryTarget, ParseError> {
    let target_refs: &[AssignTarget] = match target {
        ForTarget::Single(target) => std::slice::from_ref(target),
        ForTarget::List(targets) => targets,
    };

    let mut targets = target_refs
        .iter()
        .map(|target| {
            resolve_assign_target(target, env)
                .map(|(slot, ty)| CompiledSelectIntoTarget { slot, ty })
        })
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

    if let ([target], Some(plan)) = (targets.as_mut_slice(), static_plan)
        && target.ty.kind == SqlTypeKind::Record
    {
        let descriptor = assign_anonymous_record_descriptor(
            plan.columns()
                .into_iter()
                .map(|column| (column.name, column.sql_type))
                .collect(),
        );
        let ty = descriptor.sql_type();
        env.update_slot_type(target.slot, ty);
        target.ty = ty;
    }

    Ok(CompiledForQueryTarget { targets })
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
        other => Err(ParseError::UnexpectedToken {
            expected: "INSERT/UPDATE/DELETE ... RETURNING ... INTO",
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
        .map(|target| {
            resolve_assign_target(target, env)
                .map(|(slot, ty)| CompiledSelectIntoTarget { slot, ty })
        })
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
    let parsed = normalize_plpgsql_expr(parse_expr(&rewritten_sql)?, env);
    let (expr, sql_type) = bind_scalar_expr_in_named_slot_scope(
        &parsed,
        &env.relation_slot_scopes(),
        &env.slot_columns(),
        catalog,
        &env.local_ctes,
    )?;
    let _ = sql_type;
    let mut subplans = Vec::new();
    let expr = finalize_expr_subqueries(expr, catalog, &mut subplans);
    Ok(CompiledExpr::Scalar { expr, subplans })
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
                    CompiledExpr::Scalar { expr, subplans } if subplans.is_empty() => expr,
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

fn normalize_plpgsql_expr(expr: SqlExpr, env: &CompileEnv) -> SqlExpr {
    match expr {
        SqlExpr::Column(name) => {
            if let Some(expr) = normalize_labeled_column_name(&name, env) {
                return expr;
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
    let (label, qualifier) = label_and_var.rsplit_once('.')?;
    if let Some(scope_var) = env.get_labeled_var(label, qualifier)
        && matches!(
            scope_var.ty.kind,
            SqlTypeKind::Record | SqlTypeKind::Composite
        )
    {
        return Some(SqlExpr::FieldSelect {
            expr: Box::new(SqlExpr::Column(qualifier.to_string())),
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
        && let Some((qualifier, nested_field)) = field.rsplit_once('.')
    {
        if let Some(scope_var) = env.get_labeled_var(label, qualifier)
            && matches!(
                scope_var.ty.kind,
                SqlTypeKind::Record | SqlTypeKind::Composite
            )
        {
            return Some(SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column(qualifier.to_string())),
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
                scope_var.ty.kind,
                SqlTypeKind::Record | SqlTypeKind::Composite
            )
        {
            return Some(SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column(qualifier.to_string())),
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
            scope_var.ty.kind,
            SqlTypeKind::Record | SqlTypeKind::Composite
        )
    {
        return Some(SqlExpr::FieldSelect {
            expr: Box::new(SqlExpr::Column(qualifier.clone())),
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
        .map(|expr| normalize_plpgsql_expr(expr, env))
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

fn normalize_plpgsql_cte_body(body: CteBody, env: &CompileEnv) -> CteBody {
    match body {
        CteBody::Select(select) => {
            CteBody::Select(Box::new(normalize_plpgsql_select(*select, env)))
        }
        CteBody::Values(values) => CteBody::Values(normalize_plpgsql_values(values, env)),
        CteBody::Insert(insert) => {
            CteBody::Insert(Box::new(normalize_plpgsql_insert(*insert, env)))
        }
        CteBody::RecursiveUnion {
            all,
            anchor,
            recursive,
        } => CteBody::RecursiveUnion {
            all,
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
    stmt.returning = stmt
        .returning
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

fn resolve_assign_target(
    target: &AssignTarget,
    env: &CompileEnv,
) -> Result<(usize, SqlType), ParseError> {
    match target {
        AssignTarget::Name(name) => env
            .get_var(name)
            .map(|var| (var.slot, var.ty))
            .ok_or_else(|| ParseError::UnknownColumn(name.clone())),
        AssignTarget::Field { relation, field } => env
            .get_relation_field(relation, field)
            .map(|column| (column.slot, column.sql_type))
            .ok_or_else(|| ParseError::UnknownColumn(format!("{relation}.{field}"))),
    }
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
        env.define_var("item", SqlType::record(RECORD_TYPE_OID));
        env.push_label_scope("outer");

        let parsed = parse_expr("\"outer\".item.note").unwrap();
        assert_eq!(
            normalize_plpgsql_expr(parsed, &env),
            SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column("item".into())),
                field: "note".into(),
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
