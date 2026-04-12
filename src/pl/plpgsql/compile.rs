use std::collections::HashMap;

use crate::backend::executor::Expr;
use crate::backend::parser::{
    CatalogLookup, ParseError, SqlType, SqlTypeKind, bind_scalar_expr_in_scope, parse_expr,
    parse_type_name,
};

use super::ast::{Block, RaiseLevel, Stmt, VarDecl};

#[derive(Debug, Clone)]
pub(crate) struct CompiledBlock {
    pub(crate) local_slots: Vec<CompiledVar>,
    pub(crate) statements: Vec<CompiledStmt>,
    pub(crate) total_slots: usize,
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
}

#[derive(Debug, Clone)]
struct ScopeVar {
    slot: usize,
    ty: SqlType,
}

#[derive(Debug, Clone, Default)]
struct CompileEnv {
    vars: HashMap<String, ScopeVar>,
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
}

pub(crate) fn compile_do_block(
    block: &Block,
    catalog: &dyn CatalogLookup,
) -> Result<CompiledBlock, ParseError> {
    let mut env = CompileEnv::default();
    compile_block(block, catalog, &mut env)
}

fn compile_block(
    block: &Block,
    catalog: &dyn CatalogLookup,
    outer: &mut CompileEnv,
) -> Result<CompiledBlock, ParseError> {
    let mut env = outer.child();
    let mut local_slots = Vec::with_capacity(block.declarations.len());
    for decl in &block.declarations {
        local_slots.push(compile_var_decl(decl, catalog, &mut env)?);
    }
    let statements = block
        .statements
        .iter()
        .map(|stmt| compile_stmt(stmt, catalog, &mut env))
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
) -> Result<CompiledStmt, ParseError> {
    Ok(match stmt {
        Stmt::Block(block) => CompiledStmt::Block(compile_block(block, catalog, env)?),
        Stmt::Assign { name, expr } => {
            let var = env
                .get_var(name)
                .ok_or_else(|| ParseError::UnknownColumn(name.clone()))?;
            CompiledStmt::Assign {
                slot: var.slot,
                ty: var.ty,
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
                        compile_stmt_list(body, catalog, env)?,
                    ))
                })
                .collect::<Result<_, ParseError>>()?,
            else_branch: compile_stmt_list(else_branch, catalog, env)?,
        },
        Stmt::ForInt {
            var_name,
            start_expr,
            end_expr,
            body,
        } => {
            let mut loop_env = env.child();
            let slot = loop_env.define_var(var_name, SqlType::new(SqlTypeKind::Int4));
            let body = compile_stmt_list(body, catalog, &mut loop_env)?;
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
    })
}

fn compile_stmt_list(
    statements: &[Stmt],
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<Vec<CompiledStmt>, ParseError> {
    statements
        .iter()
        .map(|stmt| compile_stmt(stmt, catalog, env))
        .collect()
}

fn compile_expr_text(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledExpr, ParseError> {
    let parsed = parse_expr(sql)?;
    let (expr, sql_type) = bind_scalar_expr_in_scope(&parsed, &env.visible_columns(), catalog)?;
    let _ = sql_type;
    Ok(CompiledExpr { expr })
}

#[allow(dead_code)]
pub(crate) fn compile_decl_type(type_name: &str) -> Result<SqlType, ParseError> {
    parse_type_name(type_name)
}
