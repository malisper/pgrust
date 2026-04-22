#![allow(dead_code)]

use std::collections::HashMap;

use crate::backend::executor::Expr;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{
    BoundDeleteStatement, BoundInsertStatement, BoundUpdateStatement, CatalogLookup, ParseError,
    SlotScopeColumn, SqlType, SqlTypeKind, Statement, bind_delete_with_outer_scopes,
    bind_insert_with_outer_scopes, bind_update_with_outer_scopes,
    bind_scalar_expr_in_named_slot_scope, parse_expr, parse_statement, parse_type_name,
    pg_plan_query_with_outer_scopes, pg_plan_values_query_with_outer_scopes, SqlExpr,
};
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::backend::catalog::catalog::column_desc;
use crate::include::catalog::{PgProcRow, RECORD_TYPE_OID};
use crate::include::nodes::plannodes::PlannedStmt;
use crate::include::nodes::primnodes::{QueryColumn, Var, user_attrno};

use super::ast::{AssignTarget, Block, Decl, RaiseLevel, ReturnQueryKind, Stmt, VarDecl};
use super::gram::parse_block;

#[derive(Debug, Clone)]
pub(crate) struct CompiledBlock {
    pub(crate) local_slots: Vec<CompiledVar>,
    pub(crate) statements: Vec<CompiledStmt>,
    pub(crate) total_slots: usize,
}

#[derive(Debug, Clone)]
pub struct CompiledFunction {
    pub(crate) parameter_slots: Vec<CompiledFunctionSlot>,
    pub(crate) output_slots: Vec<CompiledOutputSlot>,
    pub(crate) body: CompiledBlock,
    pub(crate) return_contract: FunctionReturnContract,
    pub(crate) found_slot: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledFunctionSlot {
    pub(crate) name: String,
    pub(crate) slot: usize,
    pub(crate) ty: SqlType,
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
pub(crate) struct CompiledExpr {
    pub(crate) expr: Expr,
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
    ForInt {
        slot: usize,
        start_expr: CompiledExpr,
        end_expr: CompiledExpr,
        body: Vec<CompiledStmt>,
    },
    Raise {
        level: RaiseLevel,
        message: String,
        params: Vec<CompiledExpr>,
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
    SelectInto {
        plan: PlannedStmt,
        target_slot: usize,
        target_ty: SqlType,
    },
    ExecInsert {
        stmt: BoundInsertStatement,
    },
    ExecUpdate {
        stmt: BoundUpdateStatement,
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
}

#[derive(Debug, Clone, Default)]
struct CompileEnv {
    vars: HashMap<String, ScopeVar>,
    relation_scopes: Vec<RelationScopeVar>,
    parameter_slots: Vec<ScopeVar>,
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

    fn get_parameter(&self, index: usize) -> Option<&ScopeVar> {
        self.parameter_slots.get(index.saturating_sub(1))
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
        });
        CompiledTriggerRelation { slots, field_names }
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

    let return_contract = function_return_contract(row, catalog, &output_slots)?;
    let body = compile_block(&block, catalog, &mut env, Some(&return_contract))?;
    Ok(CompiledFunction {
        parameter_slots,
        output_slots,
        body,
        return_contract,
        found_slot,
    })
}

pub(crate) fn compile_trigger_function_from_proc(
    row: &PgProcRow,
    relation_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<CompiledFunction, ParseError> {
    let block = parse_block(&row.prosrc)?;
    let mut env = CompileEnv::default();
    let bindings = seed_trigger_env(&mut env, relation_desc);
    let found_slot = env.define_var("found", SqlType::new(SqlTypeKind::Bool));
    let return_contract = FunctionReturnContract::Trigger { bindings };
    let body = compile_block(&block, catalog, &mut env, Some(&return_contract))?;
    Ok(CompiledFunction {
        parameter_slots: Vec::new(),
        output_slots: Vec::new(),
        body,
        return_contract,
        found_slot,
    })
}

fn function_return_contract(
    row: &PgProcRow,
    catalog: &dyn CatalogLookup,
    output_slots: &[CompiledOutputSlot],
) -> Result<FunctionReturnContract, ParseError> {
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
                output_slot: None,
            },
        });
    }

    match result_type.kind {
        SqlTypeKind::Trigger => Err(ParseError::FeatureNotSupported(
            "trigger functions cannot be called in SQL expressions".into(),
        )),
        SqlTypeKind::Record | SqlTypeKind::Composite => Err(ParseError::FeatureNotSupported(
            "non-set record/composite returns are not supported yet".into(),
        )),
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
            Decl::Alias(decl) => compile_alias_decl(decl, &mut env)?,
        }
    }
    let statements = block
        .statements
        .iter()
        .map(|stmt| compile_stmt(stmt, catalog, &mut env, return_contract))
        .collect::<Result<Vec<_>, _>>()?;
    outer.next_slot = outer.next_slot.max(env.next_slot);
    Ok(CompiledBlock {
        local_slots,
        statements,
        total_slots: outer.next_slot,
    })
}

fn compile_var_decl(
    decl: &VarDecl,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledVar, ParseError> {
    let slot = env.define_var(&decl.name, decl.ty);
    let default_expr = decl
        .default_expr
        .as_deref()
        .map(|expr| compile_expr_text(expr, catalog, env))
        .transpose()?;
    Ok(CompiledVar {
        slot,
        ty: decl.ty,
        default_expr,
    })
}

fn compile_alias_decl(
    decl: &super::ast::AliasDecl,
    env: &mut CompileEnv,
) -> Result<(), ParseError> {
    let parameter = env
        .get_parameter(decl.param_index)
        .cloned()
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "function parameter referenced by ALIAS FOR",
            actual: format!("${}", decl.param_index),
        })?;
    env.define_alias(&decl.name, parameter.slot, parameter.ty);
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
                        compile_expr_text(condition, catalog, env)?,
                        compile_stmt_list(body, catalog, env, return_contract)?,
                    ))
                })
                .collect::<Result<_, ParseError>>()?,
            else_branch: compile_stmt_list(else_branch, catalog, env, return_contract)?,
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
        Stmt::Raise {
            level,
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
                message: message.clone(),
                params: params
                    .iter()
                    .map(|expr| compile_expr_text(expr, catalog, env))
                    .collect::<Result<_, _>>()?,
            }
        }
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
            if expr.trim().eq_ignore_ascii_case("new") =>
        {
            Ok(CompiledStmt::ReturnTriggerRow {
                row: TriggerReturnedRow::New,
            })
        }
        (FunctionReturnContract::Trigger { .. }, Some(expr))
            if expr.trim().eq_ignore_ascii_case("old") =>
        {
            Ok(CompiledStmt::ReturnTriggerRow {
                row: TriggerReturnedRow::Old,
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

    let planned = match parse_statement(sql)? {
        Statement::Select(stmt) => {
            pg_plan_query_with_outer_scopes(&stmt, catalog, &[outer_scope_for_sql(env)])?
        }
        Statement::Values(stmt) => {
            pg_plan_values_query_with_outer_scopes(&stmt, catalog, &[outer_scope_for_sql(env)])?
        }
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
    let planned = pg_plan_query_with_outer_scopes(
        &crate::backend::parser::parse_select(&format!("select {sql}"))?,
        catalog,
        &[outer_scope_for_sql(env)],
    )?;
    Ok(CompiledStmt::Perform { plan: planned })
}

fn compile_exec_sql_stmt(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    if let Some((target_name, select_sql)) = split_select_into_target(sql) {
        let outer_scope = outer_scope_for_sql(env);
        let target = env
            .get_var(&target_name)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "declared SELECT INTO target",
                actual: target_name.clone(),
            })?
            .clone();
        let planned = pg_plan_query_with_outer_scopes(
            &crate::backend::parser::parse_select(&select_sql)?,
            catalog,
            &[outer_scope],
        )?;
        let target_ty = if target.ty.kind == SqlTypeKind::Record {
            let descriptor = assign_anonymous_record_descriptor(
                planned
                    .columns()
                    .into_iter()
                    .map(|column| (column.name, column.sql_type))
                    .collect(),
            );
            let ty = descriptor.sql_type();
            env.update_slot_type(target.slot, ty);
            ty
        } else {
            target.ty
        };
        return Ok(CompiledStmt::SelectInto {
            plan: planned,
            target_slot: target.slot,
            target_ty,
        });
    }

    let stmt = parse_statement(sql)?;
    let outer_scope = outer_scope_for_sql(env);
    match stmt {
        Statement::Select(stmt) => Ok(CompiledStmt::Perform {
            plan: pg_plan_query_with_outer_scopes(&stmt, catalog, &[outer_scope])?,
        }),
        Statement::Values(stmt) => Ok(CompiledStmt::Perform {
            plan: pg_plan_values_query_with_outer_scopes(&stmt, catalog, &[outer_scope])?,
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
        other => Err(ParseError::UnexpectedToken {
            expected: "PL/pgSQL SQL statement",
            actual: format!("{other:?}"),
        }),
    }
}

fn outer_scope_for_sql(env: &CompileEnv) -> crate::backend::parser::BoundScope {
    let columns = env.slot_columns();
    let desc = RelationDesc {
        columns: columns
            .iter()
            .map(|column| column_desc(column.name.clone(), column.sql_type, true))
            .collect(),
    };
    crate::backend::parser::BoundScope {
        output_exprs: columns
            .iter()
            .map(|column| {
                Expr::Var(Var {
                    varno: 1,
                    varattno: user_attrno(column.slot),
                    varlevelsup: 0,
                    vartype: column.sql_type,
                })
            })
            .collect(),
        desc,
        columns: columns
            .into_iter()
            .map(|column| crate::backend::parser::analyze::ScopeColumn {
                output_name: column.name,
                hidden: column.hidden,
                qualified_only: false,
                relation_names: Vec::new(),
                hidden_invalid_relation_names: Vec::new(),
                hidden_missing_relation_names: Vec::new(),
            })
            .collect(),
        relations: Vec::new(),
    }
}

fn split_select_into_target(sql: &str) -> Option<(String, String)> {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("select into ") {
        return None;
    }
    let rest = trimmed[12..].trim_start();
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
    let select_sql = format!("select {}", rest[end..].trim_start());
    Some((target, select_sql))
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
    let parsed = normalize_plpgsql_expr(parse_expr(sql)?, env);
    let (expr, sql_type) = bind_scalar_expr_in_named_slot_scope(
        &parsed,
        &env.relation_slot_scopes(),
        &env.slot_columns(),
        catalog,
    )?;
    let _ = sql_type;
    Ok(CompiledExpr { expr })
}

fn normalize_plpgsql_expr(expr: SqlExpr, env: &CompileEnv) -> SqlExpr {
    match expr {
        SqlExpr::Column(name) => {
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
        SqlExpr::Negate(inner) => {
            SqlExpr::Negate(Box::new(normalize_plpgsql_expr(*inner, env)))
        }
        SqlExpr::BitNot(inner) => {
            SqlExpr::BitNot(Box::new(normalize_plpgsql_expr(*inner, env)))
        }
        SqlExpr::Subscript { expr, index } => SqlExpr::Subscript {
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
            index,
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
        SqlExpr::And(left, right) => SqlExpr::And(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Or(left, right) => SqlExpr::Or(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Not(inner) => SqlExpr::Not(Box::new(normalize_plpgsql_expr(*inner, env))),
        SqlExpr::IsNull(inner) => {
            SqlExpr::IsNull(Box::new(normalize_plpgsql_expr(*inner, env)))
        }
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
        SqlExpr::FieldSelect { expr, field } => SqlExpr::FieldSelect {
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
            field,
        },
        other => other,
    }
}

fn seed_trigger_env(env: &mut CompileEnv, relation_desc: &RelationDesc) -> CompiledTriggerBindings {
    let new_row = env.define_relation_scope("new", relation_desc);
    let old_row = env.define_relation_scope("old", relation_desc);
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
