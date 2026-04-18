use std::collections::HashMap;

use crate::backend::executor::Expr;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{
    CatalogLookup, ParseError, SlotScopeColumn, SqlType, SqlTypeKind, Statement,
    bind_scalar_expr_in_named_slot_scope, parse_expr, parse_statement, parse_type_name,
    pg_plan_query_with_outer, pg_plan_values_query_with_outer,
};
use crate::include::catalog::{PgProcRow, RECORD_TYPE_OID};
use crate::include::nodes::plannodes::PlannedStmt;
use crate::include::nodes::primnodes::QueryColumn;

use super::ast::{AssignTarget, Block, RaiseLevel, ReturnQueryKind, Stmt, VarDecl};
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

    fn get_var(&self, name: &str) -> Option<&ScopeVar> {
        self.vars.get(&name.to_ascii_lowercase())
    }

    fn define_relation_scope(
        &mut self,
        name: &str,
        desc: &RelationDesc,
    ) -> CompiledTriggerRelation {
        let mut slots = Vec::with_capacity(desc.columns.len());
        let mut columns = Vec::with_capacity(desc.columns.len());
        for column in &desc.columns {
            let slot = self.next_slot;
            self.next_slot += 1;
            slots.push(slot);
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
        CompiledTriggerRelation { slots }
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
                    let slot = env.define_var(&name, sql_type);
                    parameter_slots.push(CompiledFunctionSlot {
                        name,
                        slot,
                        ty: sql_type,
                    });
                }
                b'b' => {
                    let slot = env.define_var(&name, sql_type);
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
                    let slot = env.define_var(&name, sql_type);
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
            let slot = env.define_var(&name, sql_type);
            parameter_slots.push(CompiledFunctionSlot {
                name,
                slot,
                ty: sql_type,
            });
        }
    }

    let return_contract = function_return_contract(row, catalog, &output_slots)?;
    let body = compile_block(&block, catalog, &mut env, Some(&return_contract))?;
    Ok(CompiledFunction {
        parameter_slots,
        output_slots,
        body,
        return_contract,
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
    let return_contract = FunctionReturnContract::Trigger { bindings };
    let body = compile_block(&block, catalog, &mut env, Some(&return_contract))?;
    Ok(CompiledFunction {
        parameter_slots: Vec::new(),
        output_slots: Vec::new(),
        body,
        return_contract,
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
    let mut local_slots = Vec::with_capacity(block.declarations.len());
    for decl in &block.declarations {
        local_slots.push(compile_var_decl(decl, catalog, &mut env)?);
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
            pg_plan_query_with_outer(&stmt, catalog, &env.visible_columns())?
        }
        Statement::Values(stmt) => {
            pg_plan_values_query_with_outer(&stmt, catalog, &env.visible_columns())?
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
    let parsed = parse_expr(sql)?;
    let (expr, sql_type) = bind_scalar_expr_in_named_slot_scope(
        &parsed,
        &env.relation_slot_scopes(),
        &env.slot_columns(),
        catalog,
    )?;
    let _ = sql_type;
    Ok(CompiledExpr { expr })
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
