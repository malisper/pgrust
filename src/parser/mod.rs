pub use crate::catalog::{Catalog, CatalogEntry};

use crate::RelFileLocator;
use crate::catalog::column_desc;
use crate::executor::{AggAccum, AggFunc, ColumnDesc, Expr, Plan, RelationDesc, TargetEntry, Value};
use pest::Parser as _;
use pest::iterators::Pair;
use pest_derive::Parser;
use std::fmt;

#[derive(Parser)]
#[grammar = "parser/sql.pest"]
struct SqlParser;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    UnexpectedEof,
    UnexpectedToken {
        expected: &'static str,
        actual: String,
    },
    InvalidInteger(String),
    UnknownTable(String),
    UnknownColumn(String),
    EmptySelectList,
    UnsupportedQualifiedName(String),
    InvalidInsertTargetCount {
        expected: usize,
        actual: usize,
    },
    TableAlreadyExists(String),
    TableDoesNotExist(String),
    UnsupportedType(String),
    UngroupedColumn(String),
    AggInWhere,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnexpectedEof => write!(f, "unexpected end of input"),
            ParseError::UnexpectedToken { actual, .. } => write!(f, "{actual}"),
            ParseError::InvalidInteger(value) => write!(f, "invalid integer: {value}"),
            ParseError::UnknownTable(name) => write!(f, "unknown table: {name}"),
            ParseError::UnknownColumn(name) => write!(f, "unknown column: {name}"),
            ParseError::EmptySelectList => write!(f, "SELECT requires a target list or FROM clause"),
            ParseError::UnsupportedQualifiedName(name) => {
                write!(f, "unsupported qualified name: {name}")
            }
            ParseError::InvalidInsertTargetCount { expected, actual } => write!(
                f,
                "INSERT has {actual} values but target list requires {expected}"
            ),
            ParseError::TableAlreadyExists(name) => write!(f, "table already exists: {name}"),
            ParseError::TableDoesNotExist(name) => write!(f, "table does not exist: {name}"),
            ParseError::UnsupportedType(name) => write!(f, "unsupported type: {name}"),
            ParseError::UngroupedColumn(name) => {
                write!(f, "column \"{name}\" must appear in the GROUP BY clause or be used in an aggregate function")
            }
            ParseError::AggInWhere => {
                write!(f, "aggregate functions are not allowed in WHERE")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Explain(ExplainStatement),
    Select(SelectStatement),
    ShowTables,
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainStatement {
    pub analyze: bool,
    pub buffers: bool,
    pub statement: Box<Statement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStatement {
    pub from: Option<FromItem>,
    pub targets: Vec<SelectItem>,
    pub where_clause: Option<SqlExpr>,
    pub group_by: Vec<SqlExpr>,
    pub having: Option<SqlExpr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FromItem {
    Table(String),
    InnerJoin {
        left_table: String,
        right_table: String,
        on: SqlExpr,
    },
    CrossJoin {
        left_table: String,
        right_table: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectItem {
    pub output_name: String,
    pub expr: SqlExpr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByItem {
    pub expr: SqlExpr,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertStatement {
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<SqlExpr>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableStatement {
    pub table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: SqlType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlType {
    Int4,
    Text,
    Bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateStatement {
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<SqlExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStatement {
    pub table_name: String,
    pub where_clause: Option<SqlExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub column: String,
    pub expr: SqlExpr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlExpr {
    Column(String),
    Const(Value),
    Add(Box<SqlExpr>, Box<SqlExpr>),
    Eq(Box<SqlExpr>, Box<SqlExpr>),
    Lt(Box<SqlExpr>, Box<SqlExpr>),
    Gt(Box<SqlExpr>, Box<SqlExpr>),
    And(Box<SqlExpr>, Box<SqlExpr>),
    Or(Box<SqlExpr>, Box<SqlExpr>),
    Not(Box<SqlExpr>),
    IsNull(Box<SqlExpr>),
    IsNotNull(Box<SqlExpr>),
    IsDistinctFrom(Box<SqlExpr>, Box<SqlExpr>),
    IsNotDistinctFrom(Box<SqlExpr>, Box<SqlExpr>),
    AggCall {
        func: AggFunc,
        arg: Option<Box<SqlExpr>>,
    },
}

#[derive(Debug, Clone)]
struct BoundScope {
    desc: RelationDesc,
    columns: Vec<ScopeColumn>,
}

#[derive(Debug, Clone)]
struct ScopeColumn {
    output_name: String,
    qualified_name: String,
}

pub fn parse_select(sql: &str) -> Result<SelectStatement, ParseError> {
    let stmt = parse_statement(sql)?;
    match stmt {
        Statement::Select(stmt) => Ok(stmt),
        other => Err(ParseError::UnexpectedToken {
            expected: "SELECT",
            actual: format!("{other:?}"),
        }),
    }
}

pub fn parse_statement(sql: &str) -> Result<Statement, ParseError> {
    SqlParser::parse(Rule::statement, sql)
        .map_err(|e| map_pest_error("statement", e))
        .and_then(|mut pairs| build_statement(pairs.next().ok_or(ParseError::UnexpectedEof)?))
}

pub fn create_relation_desc(stmt: &CreateTableStatement) -> RelationDesc {
    RelationDesc {
        columns: stmt
            .columns
            .iter()
            .map(|column| {
                column_desc(
                    column.name.clone(),
                    match column.ty {
                        SqlType::Int4 => crate::executor::ScalarType::Int32,
                        SqlType::Text => crate::executor::ScalarType::Text,
                        SqlType::Bool => crate::executor::ScalarType::Bool,
                    },
                    column.nullable,
                )
            })
            .collect(),
    }
}

pub fn bind_create_table(
    stmt: &CreateTableStatement,
    catalog: &mut Catalog,
) -> Result<CatalogEntry, ParseError> {
    catalog
        .create_table(stmt.table_name.clone(), create_relation_desc(stmt))
        .map_err(|err| match err {
            crate::catalog::CatalogError::TableAlreadyExists(name) => {
                ParseError::TableAlreadyExists(name)
            }
            crate::catalog::CatalogError::UnknownTable(name) => ParseError::TableDoesNotExist(name),
            crate::catalog::CatalogError::UnknownType(name) => ParseError::UnsupportedType(name),
            crate::catalog::CatalogError::Io(_)
            | crate::catalog::CatalogError::Corrupt(_) => ParseError::UnexpectedToken {
                expected: "valid catalog state",
                actual: "catalog error".into(),
            },
        })
}

pub fn build_plan(stmt: &SelectStatement, catalog: &Catalog) -> Result<Plan, ParseError> {
    if stmt.targets.is_empty() && stmt.from.is_none() {
        return Err(ParseError::EmptySelectList);
    }

    let (base, scope) = if let Some(from) = &stmt.from {
        bind_from_item(from, catalog)?
    } else {
        (
            Plan::Result,
            BoundScope {
                desc: RelationDesc { columns: Vec::new() },
                columns: Vec::new(),
            },
        )
    };

    if let Some(predicate) = &stmt.where_clause {
        if expr_contains_agg(predicate) {
            return Err(ParseError::AggInWhere);
        }
    }

    let mut plan = if let Some(predicate) = &stmt.where_clause {
        Plan::Filter {
            input: Box::new(base),
            predicate: bind_expr(predicate, &scope)?,
        }
    } else {
        base
    };

    let needs_agg = !stmt.group_by.is_empty()
        || targets_contain_agg(&stmt.targets)
        || stmt.having.is_some();

    if needs_agg {
        let mut aggs: Vec<(AggFunc, Option<SqlExpr>)> = Vec::new();
        for target in &stmt.targets {
            collect_aggs(&target.expr, &mut aggs);
        }
        if let Some(having) = &stmt.having {
            collect_aggs(having, &mut aggs);
        }

        let group_keys: Vec<Expr> = stmt
            .group_by
            .iter()
            .map(|e| bind_expr(e, &scope))
            .collect::<Result<_, _>>()?;

        let accumulators: Vec<AggAccum> = aggs
            .iter()
            .map(|(func, arg)| {
                Ok(AggAccum {
                    func: *func,
                    arg: arg.as_ref().map(|e| bind_expr(e, &scope)).transpose()?,
                })
            })
            .collect::<Result<_, _>>()?;

        let n_keys = group_keys.len();
        let mut output_columns = Vec::new();
        for gk in &stmt.group_by {
            output_columns.push(sql_expr_name(gk));
        }
        for (func, _) in &aggs {
            output_columns.push(func.name().to_string());
        }

        let having = stmt
            .having
            .as_ref()
            .map(|e| bind_agg_output_expr(e, &stmt.group_by, &scope, &aggs, n_keys))
            .transpose()?;

        plan = Plan::Aggregate {
            input: Box::new(plan),
            group_by: group_keys,
            accumulators,
            having,
            output_columns: output_columns.clone(),
        };

        if !stmt.order_by.is_empty() {
            plan = Plan::OrderBy {
                input: Box::new(plan),
                items: stmt
                    .order_by
                    .iter()
                    .map(|item| {
                        Ok(crate::executor::OrderByEntry {
                            expr: bind_agg_output_expr(
                                &item.expr,
                                &stmt.group_by,
                                &scope,
                                &aggs,
                                n_keys,
                            )?,
                            descending: item.descending,
                            nulls_first: item.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>, ParseError>>()?,
            };
        }

        if stmt.limit.is_some() || stmt.offset.is_some() {
            plan = Plan::Limit {
                input: Box::new(plan),
                limit: stmt.limit,
                offset: stmt.offset.unwrap_or(0),
            };
        }

        let targets: Vec<TargetEntry> = if stmt.targets.len() == 1
            && matches!(stmt.targets[0].expr, SqlExpr::Column(ref name) if name == "*")
        {
            output_columns
                .iter()
                .enumerate()
                .map(|(i, name)| TargetEntry {
                    name: name.clone(),
                    expr: Expr::Column(i),
                })
                .collect()
        } else {
            stmt.targets
                .iter()
                .map(|item| {
                    Ok(TargetEntry {
                        name: item.output_name.clone(),
                        expr: bind_agg_output_expr(
                            &item.expr,
                            &stmt.group_by,
                            &scope,
                            &aggs,
                            n_keys,
                        )?,
                    })
                })
                .collect::<Result<_, _>>()?
        };

        Ok(Plan::Projection {
            input: Box::new(plan),
            targets,
        })
    } else {
        if !stmt.order_by.is_empty() {
            plan = Plan::OrderBy {
                input: Box::new(plan),
                items: stmt
                    .order_by
                    .iter()
                    .map(|item| {
                        Ok(crate::executor::OrderByEntry {
                            expr: bind_expr(&item.expr, &scope)?,
                            descending: item.descending,
                            nulls_first: item.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>, ParseError>>()?,
            };
        }

        if stmt.limit.is_some() || stmt.offset.is_some() {
            plan = Plan::Limit {
                input: Box::new(plan),
                limit: stmt.limit,
                offset: stmt.offset.unwrap_or(0),
            };
        }

        Ok(Plan::Projection {
            input: Box::new(plan),
            targets: bind_select_targets(&stmt.targets, &scope)?,
        })
    }
}

fn bind_select_targets(
    targets: &[SelectItem],
    scope: &BoundScope,
) -> Result<Vec<TargetEntry>, ParseError> {
    if targets.len() == 1 && matches!(targets[0].expr, SqlExpr::Column(ref name) if name == "*") {
        return Ok(scope
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| TargetEntry {
                name: column.output_name.clone(),
                expr: Expr::Column(index),
            })
            .collect());
    }

    targets
        .iter()
        .map(|item| {
            Ok(TargetEntry {
                name: item.output_name.clone(),
                expr: bind_expr(&item.expr, scope)?,
            })
        })
        .collect()
}

fn bind_expr(expr: &SqlExpr, scope: &BoundScope) -> Result<Expr, ParseError> {
    Ok(match expr {
        SqlExpr::Column(name) => Expr::Column(resolve_column(scope, name)?),
        SqlExpr::Const(value) => Expr::Const(value.clone()),
        SqlExpr::Add(left, right) => Expr::Add(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::Eq(left, right) => Expr::Eq(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::Lt(left, right) => Expr::Lt(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::Gt(left, right) => Expr::Gt(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::And(left, right) => Expr::And(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::Or(left, right) => Expr::Or(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::Not(inner) => Expr::Not(Box::new(bind_expr(inner, scope)?)),
        SqlExpr::IsNull(inner) => Expr::IsNull(Box::new(bind_expr(inner, scope)?)),
        SqlExpr::IsNotNull(inner) => Expr::IsNotNull(Box::new(bind_expr(inner, scope)?)),
        SqlExpr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::AggCall { .. } => {
            return Err(ParseError::UnexpectedToken {
                expected: "non-aggregate expression",
                actual: "aggregate function".into(),
            })
        }
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundInsertStatement {
    pub rel: RelFileLocator,
    pub desc: RelationDesc,
    pub target_indexes: Vec<usize>,
    pub values: Vec<Vec<Expr>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundUpdateStatement {
    pub rel: RelFileLocator,
    pub desc: RelationDesc,
    pub assignments: Vec<BoundAssignment>,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundDeleteStatement {
    pub rel: RelFileLocator,
    pub desc: RelationDesc,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundAssignment {
    pub column_index: usize,
    pub expr: Expr,
}

pub fn bind_insert(
    stmt: &InsertStatement,
    catalog: &Catalog,
) -> Result<BoundInsertStatement, ParseError> {
    let entry = catalog
        .get(&stmt.table_name)
        .ok_or_else(|| ParseError::UnknownTable(stmt.table_name.clone()))?;
    let scope = scope_for_relation(&stmt.table_name, &entry.desc, false);

    let target_indexes = if let Some(columns) = &stmt.columns {
        columns
            .iter()
            .map(|column| resolve_column(&scope, column))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        (0..entry.desc.columns.len()).collect()
    };

    for row in &stmt.values {
        if target_indexes.len() != row.len() {
            return Err(ParseError::InvalidInsertTargetCount {
                expected: target_indexes.len(),
                actual: row.len(),
            });
        }
    }

    Ok(BoundInsertStatement {
        rel: entry.rel,
        desc: entry.desc.clone(),
        target_indexes,
        values: stmt
            .values
            .iter()
            .map(|row| {
                row.iter()
                    .map(|expr| bind_expr(expr, &scope))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?,
    })
}

pub fn bind_update(
    stmt: &UpdateStatement,
    catalog: &Catalog,
) -> Result<BoundUpdateStatement, ParseError> {
    let entry = catalog
        .get(&stmt.table_name)
        .ok_or_else(|| ParseError::UnknownTable(stmt.table_name.clone()))?;
    let scope = scope_for_relation(&stmt.table_name, &entry.desc, false);

    Ok(BoundUpdateStatement {
        rel: entry.rel,
        desc: entry.desc.clone(),
        assignments: stmt
            .assignments
            .iter()
            .map(|assignment| {
                Ok(BoundAssignment {
                    column_index: resolve_column(&scope, &assignment.column)?,
                    expr: bind_expr(&assignment.expr, &scope)?,
                })
            })
            .collect::<Result<Vec<_>, ParseError>>()?,
        predicate: stmt
            .where_clause
            .as_ref()
            .map(|expr| bind_expr(expr, &scope))
            .transpose()?,
    })
}

pub fn bind_delete(
    stmt: &DeleteStatement,
    catalog: &Catalog,
) -> Result<BoundDeleteStatement, ParseError> {
    let entry = catalog
        .get(&stmt.table_name)
        .ok_or_else(|| ParseError::UnknownTable(stmt.table_name.clone()))?;
    let scope = scope_for_relation(&stmt.table_name, &entry.desc, false);

    Ok(BoundDeleteStatement {
        rel: entry.rel,
        desc: entry.desc.clone(),
        predicate: stmt
            .where_clause
            .as_ref()
            .map(|expr| bind_expr(expr, &scope))
            .transpose()?,
    })
}

fn resolve_column(scope: &BoundScope, name: &str) -> Result<usize, ParseError> {
    if name == "*" {
        return Err(ParseError::UnexpectedToken {
            expected: "named column",
            actual: "*".into(),
        });
    }
    if name.contains('.') {
        return scope
            .columns
            .iter()
            .position(|column| column.qualified_name.eq_ignore_ascii_case(name))
            .ok_or_else(|| ParseError::UnknownColumn(name.to_string()));
    }

    let mut matches = scope
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| {
            column
                .qualified_name
                .rsplit('.')
                .next()
                .unwrap_or(&column.qualified_name)
                .eq_ignore_ascii_case(name)
        });
    let first = matches
        .next()
        .ok_or_else(|| ParseError::UnknownColumn(name.to_string()))?;
    if matches.next().is_some() {
        return Err(ParseError::UnexpectedToken {
            expected: "unambiguous column reference",
            actual: name.to_string(),
        });
    }
    Ok(first.0)
}

fn bind_from_item(stmt: &FromItem, catalog: &Catalog) -> Result<(Plan, BoundScope), ParseError> {
    match stmt {
        FromItem::Table(name) => {
            let entry = catalog
                .get(name)
                .ok_or_else(|| ParseError::UnknownTable(name.clone()))?;
            let desc = entry.desc.clone();
            Ok((
                Plan::SeqScan {
                    rel: entry.rel,
                    desc: desc.clone(),
                },
                scope_for_relation(name, &desc, false),
            ))
        }
        FromItem::InnerJoin {
            left_table,
            right_table,
            on,
        } => {
            let left_entry = catalog
                .get(left_table)
                .ok_or_else(|| ParseError::UnknownTable(left_table.clone()))?;
            let right_entry = catalog
                .get(right_table)
                .ok_or_else(|| ParseError::UnknownTable(right_table.clone()))?;
            let left_scope = scope_for_relation(left_table, &left_entry.desc, true);
            let right_scope = scope_for_relation(right_table, &right_entry.desc, true);
            let scope = combine_scopes(&left_scope, &right_scope);
            let on = bind_expr(on, &scope)?;
            Ok((
                Plan::NestedLoopJoin {
                    left: Box::new(Plan::SeqScan {
                        rel: left_entry.rel,
                        desc: left_entry.desc.clone(),
                    }),
                    right: Box::new(Plan::SeqScan {
                        rel: right_entry.rel,
                        desc: right_entry.desc.clone(),
                    }),
                    on,
                },
                scope,
            ))
        }
        FromItem::CrossJoin {
            left_table,
            right_table,
        } => {
            let left_entry = catalog
                .get(left_table)
                .ok_or_else(|| ParseError::UnknownTable(left_table.clone()))?;
            let right_entry = catalog
                .get(right_table)
                .ok_or_else(|| ParseError::UnknownTable(right_table.clone()))?;
            let left_scope = scope_for_relation(left_table, &left_entry.desc, true);
            let right_scope = scope_for_relation(right_table, &right_entry.desc, true);
            let scope = combine_scopes(&left_scope, &right_scope);
            Ok((
                Plan::NestedLoopJoin {
                    left: Box::new(Plan::SeqScan {
                        rel: left_entry.rel,
                        desc: left_entry.desc.clone(),
                    }),
                    right: Box::new(Plan::SeqScan {
                        rel: right_entry.rel,
                        desc: right_entry.desc.clone(),
                    }),
                    on: Expr::Const(Value::Bool(true)),
                },
                scope,
            ))
        }
    }
}

fn scope_for_relation(table_name: &str, desc: &RelationDesc, qualify_output: bool) -> BoundScope {
    BoundScope {
        desc: RelationDesc {
            columns: desc
                .columns
                .iter()
                .map(|column| ColumnDesc {
                    name: if qualify_output {
                        format!("{table_name}.{}", column.name)
                    } else {
                        column.name.clone()
                    },
                    storage: column.storage.clone(),
                    ty: column.ty,
                })
                .collect(),
        },
        columns: desc
            .columns
            .iter()
            .map(|column| ScopeColumn {
                output_name: if qualify_output {
                    format!("{table_name}.{}", column.name)
                } else {
                    column.name.clone()
                },
                qualified_name: format!("{table_name}.{}", column.name),
            })
            .collect(),
    }
}

fn combine_scopes(left: &BoundScope, right: &BoundScope) -> BoundScope {
    let mut desc = left.desc.clone();
    desc.columns.extend(right.desc.columns.clone());
    let mut columns = left.columns.clone();
    columns.extend(right.columns.clone());
    BoundScope { desc, columns }
}

fn expr_contains_agg(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::AggCall { .. } => true,
        SqlExpr::Column(_) | SqlExpr::Const(_) => false,
        SqlExpr::Add(l, r)
        | SqlExpr::Eq(l, r)
        | SqlExpr::Lt(l, r)
        | SqlExpr::Gt(l, r)
        | SqlExpr::And(l, r)
        | SqlExpr::Or(l, r)
        | SqlExpr::IsDistinctFrom(l, r)
        | SqlExpr::IsNotDistinctFrom(l, r) => expr_contains_agg(l) || expr_contains_agg(r),
        SqlExpr::Not(inner) | SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) => {
            expr_contains_agg(inner)
        }
    }
}

fn targets_contain_agg(targets: &[SelectItem]) -> bool {
    targets.iter().any(|t| expr_contains_agg(&t.expr))
}

fn collect_aggs(expr: &SqlExpr, aggs: &mut Vec<(AggFunc, Option<SqlExpr>)>) {
    match expr {
        SqlExpr::AggCall { func, arg } => {
            let entry = (*func, arg.as_deref().cloned());
            if !aggs.contains(&entry) {
                aggs.push(entry);
            }
        }
        SqlExpr::Column(_) | SqlExpr::Const(_) => {}
        SqlExpr::Add(l, r)
        | SqlExpr::Eq(l, r)
        | SqlExpr::Lt(l, r)
        | SqlExpr::Gt(l, r)
        | SqlExpr::And(l, r)
        | SqlExpr::Or(l, r)
        | SqlExpr::IsDistinctFrom(l, r)
        | SqlExpr::IsNotDistinctFrom(l, r) => {
            collect_aggs(l, aggs);
            collect_aggs(r, aggs);
        }
        SqlExpr::Not(inner) | SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) => {
            collect_aggs(inner, aggs);
        }
    }
}

fn sql_expr_name(expr: &SqlExpr) -> String {
    match expr {
        SqlExpr::Column(name) => name.clone(),
        SqlExpr::AggCall { func, .. } => func.name().to_string(),
        _ => "?column?".to_string(),
    }
}

fn bind_agg_output_expr(
    expr: &SqlExpr,
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    agg_list: &[(AggFunc, Option<SqlExpr>)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    for (i, gk) in group_by_exprs.iter().enumerate() {
        if gk == expr {
            return Ok(Expr::Column(i));
        }
    }

    match expr {
        SqlExpr::AggCall { func, arg } => {
            let entry = (*func, arg.as_deref().cloned());
            for (i, agg) in agg_list.iter().enumerate() {
                if *agg == entry {
                    return Ok(Expr::Column(n_keys + i));
                }
            }
            Err(ParseError::UnexpectedToken {
                expected: "known aggregate",
                actual: format!("{}(...)", func.name()),
            })
        }
        SqlExpr::Column(name) => {
            let col_index = resolve_column(input_scope, name)?;
            for (i, gk) in group_by_exprs.iter().enumerate() {
                if let SqlExpr::Column(gk_name) = gk {
                    if let Ok(gk_index) = resolve_column(input_scope, gk_name) {
                        if gk_index == col_index {
                            return Ok(Expr::Column(i));
                        }
                    }
                }
            }
            Err(ParseError::UngroupedColumn(name.clone()))
        }
        SqlExpr::Const(v) => Ok(Expr::Const(v.clone())),
        SqlExpr::Add(l, r) => Ok(Expr::Add(
            Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?),
            Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?),
        )),
        SqlExpr::Eq(l, r) => Ok(Expr::Eq(
            Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?),
            Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?),
        )),
        SqlExpr::Lt(l, r) => Ok(Expr::Lt(
            Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?),
            Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?),
        )),
        SqlExpr::Gt(l, r) => Ok(Expr::Gt(
            Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?),
            Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?),
        )),
        SqlExpr::And(l, r) => Ok(Expr::And(
            Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?),
            Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?),
        )),
        SqlExpr::Or(l, r) => Ok(Expr::Or(
            Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?),
            Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?),
        )),
        SqlExpr::Not(inner) => Ok(Expr::Not(Box::new(bind_agg_output_expr(
            inner, group_by_exprs, input_scope, agg_list, n_keys,
        )?))),
        SqlExpr::IsNull(inner) => Ok(Expr::IsNull(Box::new(bind_agg_output_expr(
            inner, group_by_exprs, input_scope, agg_list, n_keys,
        )?))),
        SqlExpr::IsNotNull(inner) => Ok(Expr::IsNotNull(Box::new(bind_agg_output_expr(
            inner, group_by_exprs, input_scope, agg_list, n_keys,
        )?))),
        SqlExpr::IsDistinctFrom(l, r) => Ok(Expr::IsDistinctFrom(
            Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?),
            Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?),
        )),
        SqlExpr::IsNotDistinctFrom(l, r) => Ok(Expr::IsNotDistinctFrom(
            Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?),
            Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?),
        )),
    }
}

fn map_pest_error(expected: &'static str, err: pest::error::Error<Rule>) -> ParseError {
    use pest::error::ErrorVariant;

    match err.variant {
        ErrorVariant::ParsingError { .. } => ParseError::UnexpectedToken {
            expected,
            actual: err.to_string(),
        },
        ErrorVariant::CustomError { message } => ParseError::UnexpectedToken {
            expected,
            actual: message,
        },
    }
}

fn build_statement(pair: Pair<'_, Rule>) -> Result<Statement, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::explain_stmt => Ok(Statement::Explain(build_explain(inner)?)),
        Rule::select_stmt => Ok(Statement::Select(build_select(inner)?)),
        Rule::show_tables_stmt => Ok(Statement::ShowTables),
        Rule::create_table_stmt => Ok(Statement::CreateTable(build_create_table(inner)?)),
        Rule::drop_table_stmt => Ok(Statement::DropTable(build_drop_table(inner)?)),
        Rule::insert_stmt => Ok(Statement::Insert(build_insert(inner)?)),
        Rule::update_stmt => Ok(Statement::Update(build_update(inner)?)),
        Rule::delete_stmt => Ok(Statement::Delete(build_delete(inner)?)),
        _ => Err(ParseError::UnexpectedToken {
            expected: "statement",
            actual: inner.as_str().into(),
        }),
    }
}

fn build_explain(pair: Pair<'_, Rule>) -> Result<ExplainStatement, ParseError> {
    let mut analyze = false;
    let mut buffers = false;
    let mut statement = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::kw_analyze => analyze = true,
            Rule::explain_option => match part.into_inner().next().ok_or(ParseError::UnexpectedEof)? {
                opt if opt.as_rule() == Rule::kw_analyze => analyze = true,
                opt if opt.as_rule() == Rule::kw_buffers => buffers = true,
                _ => {}
            },
            Rule::select_stmt => statement = Some(Statement::Select(build_select(part)?)),
            _ => {}
        }
    }
    Ok(ExplainStatement {
        analyze,
        buffers,
        statement: Box::new(statement.ok_or(ParseError::UnexpectedEof)?),
    })
}

fn build_select(pair: Pair<'_, Rule>) -> Result<SelectStatement, ParseError> {
    let mut targets = None;
    let mut from = None;
    let mut where_clause = None;
    let mut group_by = Vec::new();
    let mut having = None;
    let mut order_by = Vec::new();
    let mut limit = None;
    let mut offset = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::select_list => targets = Some(build_select_list(part)?),
            Rule::from_item => from = Some(build_from_item(part)?),
            Rule::expr => where_clause = Some(build_expr(part)?),
            Rule::group_by_clause => group_by = build_group_by_clause(part)?,
            Rule::having_clause => having = Some(build_having_clause(part)?),
            Rule::order_by_clause => order_by = build_order_by_clause(part)?,
            Rule::limit_clause => limit = Some(build_limit_clause(part)?),
            Rule::offset_clause => offset = Some(build_offset_clause(part)?),
            _ => {}
        }
    }
    Ok(SelectStatement {
        from,
        targets: targets.unwrap_or_default(),
        where_clause,
        group_by,
        having,
        order_by,
        limit,
        offset,
    })
}

fn build_group_by_clause(pair: Pair<'_, Rule>) -> Result<Vec<SqlExpr>, ParseError> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::expr)
        .map(build_expr)
        .collect()
}

fn build_having_clause(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let expr = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::expr)
        .ok_or(ParseError::UnexpectedEof)?;
    build_expr(expr)
}

fn build_order_by_clause(pair: Pair<'_, Rule>) -> Result<Vec<OrderByItem>, ParseError> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::order_by_item)
        .map(build_order_by_item)
        .collect()
}

fn build_order_by_item(pair: Pair<'_, Rule>) -> Result<OrderByItem, ParseError> {
    let mut expr = None;
    let mut descending = false;
    let mut nulls_first = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::expr => expr = Some(build_expr(part)?),
            Rule::kw_desc => descending = true,
            Rule::kw_asc => descending = false,
            Rule::nulls_ordering => {
                for item in part.into_inner() {
                    match item.as_rule() {
                        Rule::kw_first => nulls_first = Some(true),
                        Rule::kw_last => nulls_first = Some(false),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(OrderByItem {
        expr: expr.ok_or(ParseError::UnexpectedEof)?,
        descending,
        nulls_first,
    })
}

fn build_limit_clause(pair: Pair<'_, Rule>) -> Result<usize, ParseError> {
    build_usize_clause(pair, "LIMIT")
}

fn build_offset_clause(pair: Pair<'_, Rule>) -> Result<usize, ParseError> {
    build_usize_clause(pair, "OFFSET")
}

fn build_usize_clause(pair: Pair<'_, Rule>, expected: &'static str) -> Result<usize, ParseError> {
    let integer = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::integer)
        .ok_or(ParseError::UnexpectedEof)?;
    integer
        .as_str()
        .parse::<usize>()
        .map_err(|_| ParseError::UnexpectedToken {
            expected,
            actual: integer.as_str().into(),
        })
}

fn build_from_item(pair: Pair<'_, Rule>) -> Result<FromItem, ParseError> {
    let raw = pair.as_str().to_string();
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::table_from_item => Ok(FromItem::Table(
            inner
                .into_inner()
                .find(|part| part.as_rule() == Rule::identifier)
                .map(build_identifier)
                .ok_or(ParseError::UnexpectedEof)?,
        )),
        Rule::cross_from_item => {
            let identifiers = inner
                .into_inner()
                .filter(|part| part.as_rule() == Rule::identifier)
                .map(build_identifier)
                .collect::<Vec<_>>();
            match identifiers.as_slice() {
                [left_table, right_table] => Ok(FromItem::CrossJoin {
                    left_table: left_table.clone(),
                    right_table: right_table.clone(),
                }),
                _ => Err(ParseError::UnexpectedToken {
                    expected: "cross join from clause",
                    actual: raw,
                }),
            }
        }
        Rule::joined_from_item => {
            let mut identifiers = Vec::new();
            let mut on = None;
            for part in inner.into_inner() {
                match part.as_rule() {
                    Rule::identifier => identifiers.push(build_identifier(part)),
                    Rule::expr => on = Some(build_expr(part)?),
                    _ => {}
                }
            }
            match identifiers.as_slice() {
                [left_table, right_table] => Ok(FromItem::InnerJoin {
                    left_table: left_table.clone(),
                    right_table: right_table.clone(),
                    on: on.ok_or(ParseError::UnexpectedEof)?,
                }),
                _ => Err(ParseError::UnexpectedToken {
                    expected: "joined from clause",
                    actual: raw,
                }),
            }
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "from clause",
            actual: raw,
        }),
    }
}

fn build_insert(pair: Pair<'_, Rule>) -> Result<InsertStatement, ParseError> {
    let mut table_name = None;
    let mut columns = None;
    let mut values = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::ident_list => {
                columns = Some(part.into_inner().map(build_identifier).collect::<Vec<_>>())
            }
            Rule::values_row => values.push(build_values_row(part)?),
            _ => {}
        }
    }

    Ok(InsertStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        columns,
        values,
    })
}

fn build_create_table(pair: Pair<'_, Rule>) -> Result<CreateTableStatement, ParseError> {
    let mut table_name = None;
    let mut columns = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::column_def => columns.push(build_column_def(part)?),
            _ => {}
        }
    }
    Ok(CreateTableStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        columns,
    })
}

fn build_drop_table(pair: Pair<'_, Rule>) -> Result<DropTableStatement, ParseError> {
    let table_name = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier)
        .ok_or(ParseError::UnexpectedEof)?;
    Ok(DropTableStatement { table_name })
}

fn build_update(pair: Pair<'_, Rule>) -> Result<UpdateStatement, ParseError> {
    let mut table_name = None;
    let mut assignments = Vec::new();
    let mut where_clause = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::assignment => assignments.push(build_assignment(part)?),
            Rule::expr => where_clause = Some(build_expr(part)?),
            _ => {}
        }
    }
    Ok(UpdateStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        assignments,
        where_clause,
    })
}

fn build_delete(pair: Pair<'_, Rule>) -> Result<DeleteStatement, ParseError> {
    let mut table_name = None;
    let mut where_clause = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::expr => where_clause = Some(build_expr(part)?),
            _ => {}
        }
    }
    Ok(DeleteStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        where_clause,
    })
}

fn build_select_list(pair: Pair<'_, Rule>) -> Result<Vec<SelectItem>, ParseError> {
    let mut inner = pair.into_inner();
    let first = inner.next().ok_or(ParseError::EmptySelectList)?;
    if first.as_rule() == Rule::star {
        return Ok(vec![SelectItem {
            output_name: "*".into(),
            expr: SqlExpr::Column("*".into()),
        }]);
    }

    let mut items = Vec::new();
    {
        let expr = build_expr(first)?;
        let output_name = select_item_name(&expr, items.len());
        items.push(SelectItem { output_name, expr });
    }

    for expr_pair in inner {
        let expr = build_expr(expr_pair)?;
        let output_name = select_item_name(&expr, items.len());
        items.push(SelectItem { output_name, expr });
    }

    Ok(items)
}

fn select_item_name(expr: &SqlExpr, index: usize) -> String {
    match expr {
        SqlExpr::Column(name) => name.clone(),
        SqlExpr::AggCall { func, .. } => func.name().to_string(),
        _ => format!("expr{}", index + 1),
    }
}

fn build_values_row(pair: Pair<'_, Rule>) -> Result<Vec<SqlExpr>, ParseError> {
    pair.into_inner()
        .next()
        .ok_or(ParseError::UnexpectedEof)?
        .into_inner()
        .map(build_expr)
        .collect()
}

fn build_assignment(pair: Pair<'_, Rule>) -> Result<Assignment, ParseError> {
    let mut inner = pair.into_inner();
    Ok(Assignment {
        column: build_identifier(inner.next().ok_or(ParseError::UnexpectedEof)?),
        expr: build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?,
    })
}

fn build_column_def(pair: Pair<'_, Rule>) -> Result<ColumnDef, ParseError> {
    let mut inner = pair.into_inner();
    let name = build_identifier(inner.next().ok_or(ParseError::UnexpectedEof)?);
    let ty = build_type(inner.next().ok_or(ParseError::UnexpectedEof)?);
    let nullable = match inner.next() {
        Some(flag) => flag.as_rule() == Rule::nullable,
        None => true,
    };
    Ok(ColumnDef { name, ty, nullable })
}

fn build_type(pair: Pair<'_, Rule>) -> SqlType {
    match pair.as_str().to_ascii_lowercase().as_str() {
        "int4" | "int" | "integer" => SqlType::Int4,
        "text" => SqlType::Text,
        "bool" | "boolean" => SqlType::Bool,
        _ => unreachable!(),
    }
}

fn build_identifier(pair: Pair<'_, Rule>) -> String {
    pair.as_str().to_string()
}

fn build_expr(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    match pair.as_rule() {
        Rule::expr | Rule::or_expr | Rule::and_expr | Rule::add_expr => {
            let mut inner = pair.into_inner();
            let first = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            fold_infix(first, inner)
        }
        Rule::not_expr => {
            let mut inner = pair.into_inner();
            let first = inner.next().ok_or(ParseError::UnexpectedEof)?;
            if first.as_rule() == Rule::kw_not {
                Ok(SqlExpr::Not(Box::new(build_expr(
                    inner.next().ok_or(ParseError::UnexpectedEof)?,
                )?)))
            } else {
                build_expr(first)
            }
        }
        Rule::cmp_expr => {
            let mut inner = pair.into_inner();
            let left = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            let Some(next) = inner.next() else {
                return Ok(left);
            };

            match next.as_rule() {
                Rule::null_predicate_suffix => build_null_predicate(left, next),
                Rule::comp_op => {
                    let right = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
                    Ok(match next.as_str() {
                        "=" => SqlExpr::Eq(Box::new(left), Box::new(right)),
                        "<" => SqlExpr::Lt(Box::new(left), Box::new(right)),
                        ">" => SqlExpr::Gt(Box::new(left), Box::new(right)),
                        _ => unreachable!(),
                    })
                }
                _ => Err(ParseError::UnexpectedToken {
                    expected: "comparison",
                    actual: next.as_str().into(),
                }),
            }
        }
        Rule::primary_expr => build_expr(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?),
        Rule::agg_call => build_agg_call(pair),
        Rule::identifier => Ok(SqlExpr::Column(pair.as_str().to_string())),
        Rule::integer => pair
            .as_str()
            .parse::<i32>()
            .map(|value| SqlExpr::Const(Value::Int32(value)))
            .map_err(|_| ParseError::InvalidInteger(pair.as_str().into())),
        Rule::string_literal => Ok(SqlExpr::Const(Value::Text(unescape_string(pair.as_str())))),
        Rule::kw_null => Ok(SqlExpr::Const(Value::Null)),
        Rule::kw_true => Ok(SqlExpr::Const(Value::Bool(true))),
        Rule::kw_false => Ok(SqlExpr::Const(Value::Bool(false))),
        _ => Err(ParseError::UnexpectedToken {
            expected: "expression",
            actual: pair.as_str().into(),
        }),
    }
}

fn build_agg_call(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let mut func = None;
    let mut arg = None;
    let mut is_star = false;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::agg_func => {
                let inner = part.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
                func = Some(match inner.as_rule() {
                    Rule::kw_count => AggFunc::Count,
                    Rule::kw_sum => AggFunc::Sum,
                    Rule::kw_avg => AggFunc::Avg,
                    Rule::kw_min => AggFunc::Min,
                    Rule::kw_max => AggFunc::Max,
                    _ => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "aggregate function",
                            actual: inner.as_str().into(),
                        })
                    }
                });
            }
            Rule::star => is_star = true,
            Rule::expr => arg = Some(build_expr(part)?),
            _ => {}
        }
    }
    Ok(SqlExpr::AggCall {
        func: func.ok_or(ParseError::UnexpectedEof)?,
        arg: if is_star {
            None
        } else {
            Some(Box::new(arg.ok_or(ParseError::UnexpectedEof)?))
        },
    })
}

fn build_null_predicate(
    left: SqlExpr,
    pair: Pair<'_, Rule>,
) -> Result<SqlExpr, ParseError> {
    let pair = if pair.as_rule() == Rule::null_predicate_suffix {
        pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?
    } else {
        pair
    };
    let raw = pair.as_str().to_ascii_lowercase();
    if raw == "is null" {
        return Ok(SqlExpr::IsNull(Box::new(left)));
    }
    if raw == "is not null" {
        return Ok(SqlExpr::IsNotNull(Box::new(left)));
    }

    let mut right = None;
    let mut saw_not = false;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::expr | Rule::add_expr | Rule::primary_expr | Rule::cmp_expr => {
                right = Some(build_expr(part)?)
            }
            Rule::kw_not => saw_not = true,
            _ => {}
        }
    }

    let right = right.ok_or(ParseError::UnexpectedEof)?;
    Ok(if saw_not {
        SqlExpr::IsNotDistinctFrom(Box::new(left), Box::new(right))
    } else {
        SqlExpr::IsDistinctFrom(Box::new(left), Box::new(right))
    })
}

fn fold_infix(
    first: SqlExpr,
    mut tail: pest::iterators::Pairs<'_, Rule>,
) -> Result<SqlExpr, ParseError> {
    let mut expr = first;
    while let Some(op) = tail.next() {
        let rhs = build_expr(tail.next().ok_or(ParseError::UnexpectedEof)?)?;
        expr = match op.as_rule() {
            Rule::kw_or => SqlExpr::Or(Box::new(expr), Box::new(rhs)),
            Rule::kw_and => SqlExpr::And(Box::new(expr), Box::new(rhs)),
            Rule::add_op => SqlExpr::Add(Box::new(expr), Box::new(rhs)),
            _ => unreachable!(),
        };
    }
    Ok(expr)
}

fn unescape_string(raw: &str) -> String {
    raw[1..raw.len() - 1].replace("''", "'")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::heap::tuple::{AttributeAlign, AttributeDesc};
    use crate::executor::{ColumnDesc, ScalarType};

    fn desc() -> RelationDesc {
        RelationDesc {
            columns: vec![
                ColumnDesc {
                    name: "id".into(),
                    storage: AttributeDesc {
                        name: "id".into(),
                        attlen: 4,
                        attalign: AttributeAlign::Int,
                        nullable: false,
                    },
                    ty: ScalarType::Int32,
                },
                ColumnDesc {
                    name: "name".into(),
                    storage: AttributeDesc {
                        name: "name".into(),
                        attlen: -1,
                        attalign: AttributeAlign::Int,
                        nullable: false,
                    },
                    ty: ScalarType::Text,
                },
                ColumnDesc {
                    name: "note".into(),
                    storage: AttributeDesc {
                        name: "note".into(),
                        attlen: -1,
                        attalign: AttributeAlign::Int,
                        nullable: true,
                    },
                    ty: ScalarType::Text,
                },
            ],
        }
    }

    fn catalog() -> Catalog {
        let mut catalog = Catalog::default();
        catalog.insert(
            "people",
            CatalogEntry {
                rel: RelFileLocator {
                    spc_oid: 0,
                    db_oid: 1,
                    rel_number: 15000,
                },
                desc: desc(),
            },
        );
        catalog
    }

    #[test]
    fn pest_matches_basic_select_keyword() {
        let mut pairs = SqlParser::parse(Rule::kw_select_atom, "select").unwrap();
        assert_eq!(pairs.next().unwrap().as_str(), "select");
    }

    #[test]
    fn pest_matches_minimal_select_statement() {
        let mut pairs = SqlParser::parse(Rule::statement, "select id from people").unwrap();
        let stmt = build_statement(pairs.next().unwrap()).unwrap();
        match stmt {
            Statement::Select(stmt) => {
                assert_eq!(stmt.from, Some(FromItem::Table("people".into())));
                assert_eq!(stmt.targets.len(), 1);
            }
            other => panic!("expected select statement, got {other:?}"),
        }
    }

    #[test]
    fn parse_select_with_where() {
        let stmt =
            parse_select("select name, note from people where id > 1 and note is null").unwrap();
        assert_eq!(stmt.from, Some(FromItem::Table("people".into())));
        assert_eq!(stmt.targets.len(), 2);
        assert!(matches!(stmt.where_clause, Some(SqlExpr::And(_, _))));
    }

    #[test]
    fn parse_null_predicates() {
        let stmt = parse_select(
            "select name from people where note is not null or note is distinct from null",
        )
        .unwrap();
        assert!(matches!(stmt.where_clause, Some(SqlExpr::Or(_, _))));

        let stmt =
            parse_select("select name from people where note is not distinct from null").unwrap();
        assert!(matches!(
            stmt.where_clause,
            Some(SqlExpr::IsNotDistinctFrom(_, _))
        ));
    }

    #[test]
    fn parse_join_select() {
        let stmt = parse_select(
            "select people.name, pets.name from people join pets on people.id = pets.owner_id",
        )
        .unwrap();
        assert_eq!(
            stmt.from,
            Some(FromItem::InnerJoin {
                left_table: "people".into(),
                right_table: "pets".into(),
                on: SqlExpr::Eq(
                    Box::new(SqlExpr::Column("people.id".into())),
                    Box::new(SqlExpr::Column("pets.owner_id".into()))
                ),
            })
        );
    }

    #[test]
    fn parse_cross_join_select() {
        let stmt = parse_select("select people.name, pets.name from people, pets").unwrap();
        assert_eq!(
            stmt.from,
            Some(FromItem::CrossJoin {
                left_table: "people".into(),
                right_table: "pets".into(),
            })
        );
    }

    #[test]
    fn parse_select_without_from() {
        let stmt = parse_select("select 1").unwrap();
        assert_eq!(stmt.from, None);
        assert_eq!(stmt.targets.len(), 1);
    }

    #[test]
    fn parse_select_without_targets_but_with_from() {
        let stmt = parse_select("select from people").unwrap();
        assert_eq!(stmt.from, Some(FromItem::Table("people".into())));
        assert!(stmt.targets.is_empty());
    }

    #[test]
    fn parse_addition_in_where_clause() {
        let stmt =
            parse_select("select * from people, pets where pets.owner_id + 1 = people.id").unwrap();
        assert!(matches!(
            stmt.where_clause,
            Some(SqlExpr::Eq(left, _))
                if matches!(*left, SqlExpr::Add(_, _))
        ));
    }

    #[test]
    fn parse_select_with_order_limit_offset() {
        let stmt =
            parse_select("select name from people order by id desc limit 2 offset 1").unwrap();
        assert_eq!(stmt.order_by.len(), 1);
        assert!(stmt.order_by[0].descending);
        assert_eq!(stmt.order_by[0].nulls_first, None);
        assert_eq!(stmt.limit, Some(2));
        assert_eq!(stmt.offset, Some(1));
    }

    #[test]
    fn parse_select_with_explicit_nulls_ordering() {
        let stmt = parse_select("select name from people order by note desc nulls last").unwrap();
        assert_eq!(stmt.order_by.len(), 1);
        assert!(stmt.order_by[0].descending);
        assert_eq!(stmt.order_by[0].nulls_first, Some(false));
    }

    #[test]
    fn build_plan_resolves_columns() {
        let stmt = parse_select("select name, note from people where id > 1").unwrap();
        let plan = build_plan(&stmt, &catalog()).unwrap();
        match plan {
            Plan::Projection { input, targets } => {
                assert_eq!(targets.len(), 2);
                match *input {
                    Plan::Filter { input, predicate } => {
                        assert!(matches!(predicate, Expr::Gt(_, _)));
                        assert!(matches!(*input, Plan::SeqScan { .. }));
                    }
                    other => panic!("expected filter, got {:?}", other),
                }
            }
            other => panic!("expected projection, got {:?}", other),
        }
    }

    #[test]
    fn build_join_plan_resolves_qualified_columns() {
        let mut catalog = catalog();
        catalog.insert(
            "pets",
            CatalogEntry {
                rel: RelFileLocator {
                    spc_oid: 0,
                    db_oid: 1,
                    rel_number: 15001,
                },
                desc: RelationDesc {
                    columns: vec![
                        ColumnDesc {
                            name: "id".into(),
                            storage: AttributeDesc {
                                name: "id".into(),
                                attlen: 4,
                                attalign: AttributeAlign::Int,
                                nullable: false,
                            },
                            ty: ScalarType::Int32,
                        },
                        ColumnDesc {
                            name: "owner_id".into(),
                            storage: AttributeDesc {
                                name: "owner_id".into(),
                                attlen: 4,
                                attalign: AttributeAlign::Int,
                                nullable: false,
                            },
                            ty: ScalarType::Int32,
                        },
                    ],
                },
            },
        );

        let stmt = parse_select(
            "select people.name, pets.id from people join pets on people.id = pets.owner_id",
        )
        .unwrap();
        let plan = build_plan(&stmt, &catalog).unwrap();
        match plan {
            Plan::Projection { input, targets } => {
                assert_eq!(targets.len(), 2);
                match *input {
                    Plan::NestedLoopJoin { on, .. } => assert!(matches!(on, Expr::Eq(_, _))),
                    other => panic!("expected join, got {:?}", other),
                }
            }
            other => panic!("expected projection, got {:?}", other),
        }
    }

    #[test]
    fn unknown_column_is_rejected() {
        let stmt = parse_select("select missing from people").unwrap();
        assert!(matches!(
            build_plan(&stmt, &catalog()),
            Err(ParseError::UnknownColumn(name)) if name == "missing"
        ));
    }

    #[test]
    fn select_star_expands_to_all_columns() {
        let stmt = parse_select("select * from people").unwrap();
        let plan = build_plan(&stmt, &catalog()).unwrap();
        match plan {
            Plan::Projection { input, targets } => {
                assert_eq!(targets.len(), 3);
                assert_eq!(targets[0].name, "id");
                assert_eq!(targets[1].name, "name");
                assert_eq!(targets[2].name, "note");
                assert!(matches!(*input, Plan::SeqScan { .. }));
            }
            other => panic!("expected projection, got {:?}", other),
        }
    }

    #[test]
    fn build_plan_wraps_order_by_and_limit() {
        let stmt =
            parse_select("select name from people where id > 0 order by id desc limit 2 offset 1")
                .unwrap();
        let plan = build_plan(&stmt, &catalog()).unwrap();
        match plan {
            Plan::Projection { input, targets } => {
                assert_eq!(targets.len(), 1);
                match *input {
                    Plan::Limit { input, limit, offset } => {
                        assert_eq!(limit, Some(2));
                        assert_eq!(offset, 1);
                        match *input {
                            Plan::OrderBy { input, items } => {
                                assert_eq!(items.len(), 1);
                                assert!(items[0].descending);
                                assert!(matches!(*input, Plan::Filter { .. }));
                            }
                            other => panic!("expected order by, got {:?}", other),
                        }
                    }
                    other => panic!("expected limit, got {:?}", other),
                }
            }
            other => panic!("expected projection, got {:?}", other),
        }
    }

    #[test]
    fn parse_insert_update_delete() {
        assert!(matches!(
            parse_statement("explain select name from people").unwrap(),
            Statement::Explain(ExplainStatement { analyze: false, buffers: false, .. })
        ));
        assert!(matches!(
            parse_statement("explain analyze select name from people").unwrap(),
            Statement::Explain(ExplainStatement { analyze: true, buffers: false, .. })
        ));
        assert!(matches!(
            parse_statement("explain (analyze, buffers) select name from people").unwrap(),
            Statement::Explain(ExplainStatement { analyze: true, buffers: true, .. })
        ));
        assert!(matches!(
            parse_statement("insert into people (id, name) values (1, 'alice')").unwrap(),
            Statement::Insert(InsertStatement { table_name, .. }) if table_name == "people"
        ));
        assert!(matches!(
            parse_statement("insert into people (id, name) values (1, 'alice'), (2, 'bob')").unwrap(),
            Statement::Insert(InsertStatement { table_name, values, .. })
                if table_name == "people" && values.len() == 2
        ));
        assert!(matches!(
            parse_statement("create table widgets (id int4 not null, name text)").unwrap(),
            Statement::CreateTable(CreateTableStatement { table_name, columns })
                if table_name == "widgets" && columns.len() == 2
        ));
        assert!(matches!(
            parse_statement("drop table widgets").unwrap(),
            Statement::DropTable(DropTableStatement { table_name }) if table_name == "widgets"
        ));
        assert!(matches!(
            parse_statement("update people set note = 'x' where id = 1").unwrap(),
            Statement::Update(UpdateStatement { table_name, .. }) if table_name == "people"
        ));
        assert!(matches!(
            parse_statement("delete from people where note is null").unwrap(),
            Statement::Delete(DeleteStatement { table_name, .. }) if table_name == "people"
        ));
        assert!(matches!(
            parse_statement("show tables").unwrap(),
            Statement::ShowTables
        ));
    }

    #[test]
    fn parse_aggregate_select() {
        let stmt = parse_select("select count(*) from people").unwrap();
        assert_eq!(stmt.targets.len(), 1);
        assert!(matches!(
            stmt.targets[0].expr,
            SqlExpr::AggCall { func: AggFunc::Count, arg: None }
        ));
        assert_eq!(stmt.targets[0].output_name, "count");
    }

    #[test]
    fn parse_group_by_and_having() {
        let stmt = parse_select(
            "select name, count(*) from people group by name having count(*) > 1",
        )
        .unwrap();
        assert_eq!(stmt.group_by.len(), 1);
        assert!(matches!(stmt.group_by[0], SqlExpr::Column(ref name) if name == "name"));
        assert!(stmt.having.is_some());
    }

    #[test]
    fn build_plan_with_aggregate() {
        let stmt =
            parse_select("select name, count(*) from people group by name").unwrap();
        let plan = build_plan(&stmt, &catalog()).unwrap();
        match plan {
            Plan::Projection { input, targets } => {
                assert_eq!(targets.len(), 2);
                assert_eq!(targets[0].name, "name");
                assert_eq!(targets[1].name, "count");
                assert!(matches!(*input, Plan::Aggregate { .. }));
            }
            other => panic!("expected projection, got {:?}", other),
        }
    }

    #[test]
    fn ungrouped_column_rejected_at_plan_time() {
        let stmt = parse_select("select name, count(*) from people").unwrap();
        assert!(matches!(
            build_plan(&stmt, &catalog()),
            Err(ParseError::UngroupedColumn(name)) if name == "name"
        ));
    }

    #[test]
    fn aggregate_in_where_rejected() {
        let stmt = parse_select("select name from people where count(*) > 1").unwrap();
        assert!(matches!(
            build_plan(&stmt, &catalog()),
            Err(ParseError::AggInWhere)
        ));
    }
}
