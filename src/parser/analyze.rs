use crate::RelFileLocator;
use crate::catalog::column_desc;
use crate::executor::{AggAccum, AggFunc, ColumnDesc, Expr, Plan, RelationDesc, TargetEntry, Value};

pub use crate::catalog::{Catalog, CatalogEntry};
use super::parsenodes::*;

#[derive(Debug, Clone)]
pub(crate) struct BoundScope {
    pub(crate) desc: RelationDesc,
    pub(crate) columns: Vec<ScopeColumn>,
}

#[derive(Debug, Clone)]
pub(crate) struct ScopeColumn {
    pub(crate) output_name: String,
    pub(crate) qualified_name: String,
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
                        SqlType::Text | SqlType::Timestamp | SqlType::Char => {
                            crate::executor::ScalarType::Text
                        }
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

pub(crate) fn bind_expr(expr: &SqlExpr, scope: &BoundScope) -> Result<Expr, ParseError> {
    Ok(match expr {
        SqlExpr::Column(name) => Expr::Column(resolve_column(scope, name)?),
        SqlExpr::Const(value) => Expr::Const(value.clone()),
        SqlExpr::Add(left, right) => Expr::Add(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::Negate(inner) => Expr::Negate(Box::new(bind_expr(inner, scope)?)),
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
        SqlExpr::Random => Expr::Random,
        SqlExpr::CurrentTimestamp => Expr::CurrentTimestamp,
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
        FromItem::InnerJoin { left_table, right_table, on } => {
            let left_entry = catalog.get(left_table).ok_or_else(|| ParseError::UnknownTable(left_table.clone()))?;
            let right_entry = catalog.get(right_table).ok_or_else(|| ParseError::UnknownTable(right_table.clone()))?;
            let left_scope = scope_for_relation(left_table, &left_entry.desc, true);
            let right_scope = scope_for_relation(right_table, &right_entry.desc, true);
            let scope = combine_scopes(&left_scope, &right_scope);
            let on = bind_expr(on, &scope)?;
            Ok((
                Plan::NestedLoopJoin {
                    left: Box::new(Plan::SeqScan { rel: left_entry.rel, desc: left_entry.desc.clone() }),
                    right: Box::new(Plan::SeqScan { rel: right_entry.rel, desc: right_entry.desc.clone() }),
                    on,
                },
                scope,
            ))
        }
        FromItem::CrossJoin { left_table, right_table } => {
            let left_entry = catalog.get(left_table).ok_or_else(|| ParseError::UnknownTable(left_table.clone()))?;
            let right_entry = catalog.get(right_table).ok_or_else(|| ParseError::UnknownTable(right_table.clone()))?;
            let left_scope = scope_for_relation(left_table, &left_entry.desc, true);
            let right_scope = scope_for_relation(right_table, &right_entry.desc, true);
            let scope = combine_scopes(&left_scope, &right_scope);
            Ok((
                Plan::NestedLoopJoin {
                    left: Box::new(Plan::SeqScan { rel: left_entry.rel, desc: left_entry.desc.clone() }),
                    right: Box::new(Plan::SeqScan { rel: right_entry.rel, desc: right_entry.desc.clone() }),
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
            columns: desc.columns.iter().map(|column| ColumnDesc {
                name: if qualify_output { format!("{table_name}.{}", column.name) } else { column.name.clone() },
                storage: column.storage.clone(),
                ty: column.ty,
            }).collect(),
        },
        columns: desc.columns.iter().map(|column| ScopeColumn {
            output_name: if qualify_output { format!("{table_name}.{}", column.name) } else { column.name.clone() },
            qualified_name: format!("{table_name}.{}", column.name),
        }).collect(),
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
        SqlExpr::Column(_) | SqlExpr::Const(_) | SqlExpr::Random | SqlExpr::CurrentTimestamp => false,
        SqlExpr::Add(l, r) | SqlExpr::Eq(l, r) | SqlExpr::Lt(l, r) | SqlExpr::Gt(l, r)
        | SqlExpr::And(l, r) | SqlExpr::Or(l, r) | SqlExpr::IsDistinctFrom(l, r)
        | SqlExpr::IsNotDistinctFrom(l, r) => expr_contains_agg(l) || expr_contains_agg(r),
        SqlExpr::Negate(inner) | SqlExpr::Not(inner) | SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) => expr_contains_agg(inner),
    }
}

fn targets_contain_agg(targets: &[SelectItem]) -> bool {
    targets.iter().any(|t| expr_contains_agg(&t.expr))
}

fn collect_aggs(expr: &SqlExpr, aggs: &mut Vec<(AggFunc, Option<SqlExpr>)>) {
    match expr {
        SqlExpr::AggCall { func, arg } => {
            let entry = (*func, arg.as_deref().cloned());
            if !aggs.contains(&entry) { aggs.push(entry); }
        }
        SqlExpr::Column(_) | SqlExpr::Const(_) | SqlExpr::Random | SqlExpr::CurrentTimestamp => {}
        SqlExpr::Add(l, r) | SqlExpr::Eq(l, r) | SqlExpr::Lt(l, r) | SqlExpr::Gt(l, r)
        | SqlExpr::And(l, r) | SqlExpr::Or(l, r) | SqlExpr::IsDistinctFrom(l, r)
        | SqlExpr::IsNotDistinctFrom(l, r) => { collect_aggs(l, aggs); collect_aggs(r, aggs); }
        SqlExpr::Negate(inner) | SqlExpr::Not(inner) | SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) => collect_aggs(inner, aggs),
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
        if gk == expr { return Ok(Expr::Column(i)); }
    }

    match expr {
        SqlExpr::AggCall { func, arg } => {
            let entry = (*func, arg.as_deref().cloned());
            for (i, agg) in agg_list.iter().enumerate() {
                if *agg == entry { return Ok(Expr::Column(n_keys + i)); }
            }
            Err(ParseError::UnexpectedToken { expected: "known aggregate", actual: format!("{}(...)", func.name()) })
        }
        SqlExpr::Column(name) => {
            let col_index = resolve_column(input_scope, name)?;
            for (i, gk) in group_by_exprs.iter().enumerate() {
                if let SqlExpr::Column(gk_name) = gk {
                    if let Ok(gk_index) = resolve_column(input_scope, gk_name) {
                        if gk_index == col_index { return Ok(Expr::Column(i)); }
                    }
                }
            }
            Err(ParseError::UngroupedColumn(name.clone()))
        }
        SqlExpr::Const(v) => Ok(Expr::Const(v.clone())),
        SqlExpr::Add(l, r) => Ok(Expr::Add(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::Negate(inner) => Ok(Expr::Negate(Box::new(bind_agg_output_expr(inner, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::Eq(l, r) => Ok(Expr::Eq(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::Lt(l, r) => Ok(Expr::Lt(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::Gt(l, r) => Ok(Expr::Gt(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::And(l, r) => Ok(Expr::And(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::Or(l, r) => Ok(Expr::Or(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::Not(inner) => Ok(Expr::Not(Box::new(bind_agg_output_expr(inner, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::IsNull(inner) => Ok(Expr::IsNull(Box::new(bind_agg_output_expr(inner, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::IsNotNull(inner) => Ok(Expr::IsNotNull(Box::new(bind_agg_output_expr(inner, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::IsDistinctFrom(l, r) => Ok(Expr::IsDistinctFrom(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::IsNotDistinctFrom(l, r) => Ok(Expr::IsNotDistinctFrom(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::Random => Ok(Expr::Random),
        SqlExpr::CurrentTimestamp => Ok(Expr::CurrentTimestamp),
    }
}
